#include <algorithm>
#include <unistd.h>

#include "include/types.h"
#include "objclass/objclass.h"
#include "cls/lsm/cls_lsm_types.h"
#include "cls/lsm/cls_lsm_ops.h"
#include "cls/lsm/cls_lsm_bloomfilter.h"
#include "cls/lsm/cls_lsm_src.h"
#include "cls/lsm/cls_lsm_const.h"

using ceph::bufferlist;
using ceph::decode;
using ceph::encode;

/*
 * initializes only the root node (total == fan_out)
 */
int lsm_init(cls_method_context_t hctx, const cls_lsm_init_op& op)
{
    // check if the tree was already initialized
    cls_lsm_node_head root;
    auto ret = lsm_read_node_head(hctx, root);
    if (ret == 0) {
        CLS_LOG(5, "ERROR: tree was already initialized before");
        return -EEXIST;
    }

    // will only continue if tree was not initialized (which returns -EINVAL)
    if (ret != -EINVAL) {
        CLS_LOG(5, "ERROR: faild to initialize lsm tree");
        return ret;
    }

    // initialize the tree-config
    root.pool = op.pool_name;
    root.my_object_id = op.tree_name;
    root.my_level = 0;
    root.levels = op.levels;
    root.key_range = op.key_range;
    root.column_group_splits = op.column_group_splits;
    root.size = 0;
    root.capacity = op.capacity;
    root.bloomfilter_store_ever = std::vector<bool>(BLOOM_FILTER_STORE_SIZE_256K, false);
    root.bloomfilter_store = std::vector<bool>(BLOOM_FILTER_STORE_SIZE_256K, false);

    // total length of the tree-config = tree other fields + config_end_offset + LSM_TREE_START
    bufferlist bl;
    encode(root, bl);
    root.entry_start_offset = bl.length() + sizeof(uint64_t) * 3 + sizeof(uint16_t) + LSM_DATA_START_PADDING;
    root.entry_end_offset = root.entry_start_offset;

    // write out the tree-config
    ret = lsm_write_node_head(hctx, root);
    if (ret < 0) {
        CLS_LOG(5, "ERROR: failed to initialize lsm tree");
        return ret;
    }

    return 0;
}

/**
 * Read rows whose key matching "keys" (returning only asked columns)
 */
int lsm_read_data(cls_method_context_t hctx, cls_lsm_node_head& root, cls_lsm_get_entries_op& op, cls_lsm_get_entries_ret& op_ret)
{
    bufferlist bl_chunk;
    auto ret = cls_cxx_read(hctx, root.entry_start_offset, (root.entry_end_offset - root.entry_start_offset), &bl_chunk);
    if (ret < 0) {
        CLS_LOG(1, "ERROR: in lsm_read_data: reading entries failed");
        return ret;
    }

    ret = lsm_get_entries(&bl_chunk, op.keys, op_ret);
    if (ret < 0) {
        CLS_LOG(1, "ERROR: in lsm_read_data: getting the entries out failed");
        return ret;
    }
    
    return op_ret.entries.size();
}

/**
 * Prepare to read from gathering from children objects
 */
int lsm_read_with_gathering(cls_method_context_t hctx, cls_lsm_node_head& root, cls_lsm_get_entries_op& op, cls_lsm_get_entries_ret& op_ret)
{
    // get the child object ids to read from
    std::set<std::string> child_objs;
    lsm_get_child_object_ids(root, op.keys, op.columns, child_objs);
    child_objs.insert(root.my_object_id);

    lsm_gather(hctx, root, child_objs, op, op_ret);

    return 0;
}

/**
 * Do remote reads to gather
 */
int lsm_gather(cls_method_context_t hctx, cls_lsm_node_head root, std::set<std::string>& child_objs, cls_lsm_get_entries_op& op, cls_lsm_get_entries_ret& op_ret)
{
    std::map<std::string, bufferlist> src_obj_buffs;
    int r = cls_cxx_get_gathered_data(hctx, &src_obj_buffs);
    if (src_obj_buffs.empty()) {
        bufferlist in;
        r = cls_cxx_gather(hctx, child_objs, root.pool, LSM_CLASS, LSM_READ_FROM_INTERNAL_NODES, *in);
    } else {
        std::vector<uint64_t> found_keys;
        for (std::map<std::string, bufferlist>::iterator it = src_obj_buffs.begin(); it != src_obj_buffs.end(); it++){
            bufferlist bl= it->second;
            cls_lsm_get_entries_ret child_ret;
            r = lsm_get_entries(&bl, op.keys, child_ret);
            op_ret.entries.insert(op.entries.end(), child_ret.entries.begin(), child_ret.entries.end());
        }
    }
}

/**
 * Get Child object ids
 */
int lsm_get_child_object_ids(cls_lsm_node_head& head, std::vector<uint64_t>& keys, std::vector<std::string>& cols, std::set<std::string>& child_objs)
{
    std::set<uint16_t> col_grps;
    lsm_get_column_groups(cols, head.column_group_splits, col_grps);

    std::vector<std::vector<uint64_t>> key_groups(head.key_range.splits);
    uint64_t increment = (head.key_range.high_bound - head.key_range.high_bound) / head.key_range.splits + 1;
    for (auto key : keys) {
        uint64_t low = head.key_range.low_bound;
        uint64_t high = low + increment;

        for (int i = 0; i < head.key_range.splits; i++) {
            if (key >= low && key < high) {
                key_groups[i].push_back(key);
                break;
            }
            low = high;
            high = low + increment;
        }
    }

    for (int i = 0; i < head.key_range.splits; i++) {
        if (key_groups[i].size() > 0) {
            for (auto col_grp : col_grps) {
                std::string object_id = head.my_object_id + "/kr-" + to_string(i+1) + ":cg-" + to_string(col_grp);
                child_objs.insert(object_id);
            }  
        }
    }

    return 0;
}

/**
 * Read node head
 */
int lsm_read_node_head(cls_method_context_t hctx, cls_lsm_node_head& node_head)
{
    uint64_t read_size = CHUNK_SIZE, start_offset = 0;

    bufferlist bl_head;
    const auto ret = cls_cxx_read(hctx, start_offset, read_size, &bl_head);
    if (ret < 0) {
        CLS_LOG(1, "ERROR: lsm_read_node_head: failed read lsm node head");
        return ret;
    }
    if (ret == 0) {
        CLS_LOG(1, "INFO: lsm_read_node_head: empty node, not initialized yet");
        return -EONENT;
    }

    // Process the chunk of data read
    auto it = bl_head.cbegin();

    // Check node start
    uint16_t node_start;
    try {
        decode(node_start, it);
    } catch (const ceph::buffer::error& err) {
        CLS_LOG(0, "ERROR: lsm_read_node_head: failed to decode node start");
        return -EINVAL;
    }
    if (node_start != LSM_NODE_START) {
        CLS_LOG(0, "ERROR: lsm_read_node_head: invalid node start");
        return -EINVAL;
    }

    uint64_t encoded_len;
    try {
        decode(encoded_len, it);
    } catch (const ceph::buffer::error& err) {
        CLS_LOG(0, "ERROR: lsm_read_node_head: failed to decode encoded size: %s", err.what());
        return -EINVAL;
    }

    if (encoded_len > (read_size - LSM_NODE_OVERHEAD)) {
        start_offset = read_size;
        read_size = encoded_len - (read_size - LSM_NODE_OVERHEAD);
        bufferlist bl_remaining_head;
        const auto ret = cls_cxx_read2(hctx, start_offset, read_size, &bl_remaining_head, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
        if (ret < 0) {
            CLS_LOG(1, "ERROR: lsm_read_node_head: failed to read the remaining part of the node");
            return ret;
        }
        bl_head.claim_append(bl_remaining_head);
    }

    try {
        decode(node_head, it);
    } catch (const ceph::buffer::error& err) {
        CLS_LOG(0, "ERROR: lsm_read_node: failed to decode node: %s", err.what());
        return -EINVAL;
    }

    return 0;
}

/**
 * As part of read, this checks the bloom filter stored in node head to see if a key is stored
 * in the object.
 */
void lsm_check_if_key_exists(std::vector<bool>& bloomfilter_store, std::vector<uint64_t>& search_keys, std::vector<uint64_t>& found_keys)
{
    for (auto key : search_keys) {
        if (lsm_bloomfilter_contains(bloomfilter_store, to_string(key))) {
            found_keys.push_back(key);
        }
    }
}

/**
* get column group numbers for children objects for a read query
*/
int lsm_get_column_groups(std::vector<std::string>& cols, std::vector<std::set<std::string>>& column_group_splits, std::set<uint16_t>& col_grps)
{
    if (col_grps.size() > 0) {
        CLS_LOG(1, "Error: there was data in column groups to return");
        return -EINVAL;
    }

    for (auto col : cols) {
        if (col_grps.size() == column_group_splits.size()) {
            break;
        }

        for (uint64_t i = 0; i < column_group_splits.size(); i++) {
            if (column_group_splits[i].find(col) != column_group_splits[i].end()) {
                col_grps.insert(i+1);
                break;
            }
        }
    }

    return 0;
}

/**
 * Read all entries from object
 */
int lsm_get_entries(bufferlist *in, std::vector<uint64_t>& read_keys, cls_lsm_get_entries_ret& op_ret)
{
    bool read_all = true;
    if (!read_keys.empty()) {
        read_all = false;
    }

    uint16_t entry_start = 0;
    uint64_t data_size = 0;
    auto it = in->cbegin();
    uint64_t size_to_process = in->length();

    do {
        cls_lsm_entry entry;
        try {
            decode(entry_start, it);
        } catch (const ceph::buffer::error& err) {
            CLS_LOG(10, "ERROR: lsm_get_entries: failed to decode entry start: %s", err.what());
            return -EINVAL;
        }

        if (entry_start != LSM_ENTRY_START) {
            CLS_LOG(5, "ERROR: lsm_get_entries: invalid entry start %u", entry_start);
            return -EINVAL;
        }
        size_to_process -= sizeof(uint16_t);

        try {
            decode(data_size, it);
        } catch (const ceph::buffer::error& err) {
            CLS_LOG(10, "ERROR: lsm_get_entries: failed to decode data size: %s", err.what());
            return -EINVAL;
        }
        CLS_LOG(1, "INFO: lsm_get_entries: data size %lu", data_size);
        size_to_process -= sizeof(uint64_t);

        if (data_size > size_to_process) {
            CLS_LOG(10, "INFO: lsm_get_entries: not enough data to read, breaking the loop...");
            break;
        }

        try {
            decode(entry, it);
        } catch (const ceph::buffer::error& err) {
            CLS_LOG(10, "ERROR: lsm_retrieve_data: failed to decode entry %s", err.what());
            return -EINVAL;
        }
        size_to_process -= data_size;

        // check if this is the entry wanted
        if (!read_all) {
            auto it = std::find(read_keys.begin(), read_keys.end(), entry.key);
            if (it != read_keys.end()) {
                op_ret.entries.emplace_back(entry);
                read_keys.erase(it);
            }

            // if we have found all we need, break
            if (read_keys.empty()) {
                break;
            }
        } else {
            op_ret.entries.emplace_back(entry);
        }
    } while (size_to_process > 0);

    return 0;
}

/**
 * Write an entry into an object
 */
int lsm_write_entry(cls_method_context_t hctx, cls_lsm_entry& entry, uint64_t start_offset)
{
    bufferlist bl;
    uint16_t entry_start = LSM_ENTRY_START;
    encode(entry_start, bl);

    bufferlist bl_data;
    encode(entry, bl_data);
    uint64_t encoded_len = bl_data.length();
    encode(encoded_len, bl);

    bl.claim_append(bl_data);

    // then we will write the data into the object
    auto ret = cls_cxx_write2(hctx, start_offset, bl.length(), &bl, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
    if (ret < 0) {
        CLS_LOG(1, "ERROR: lsm_write_entry: failed to write entry into the object");
        return ret;
    }

    return bl.length();
}

/**
 * Write the head of the object node
 */
int lsm_write_node_head(cls_method_context_t hctx, cls_lsm_node_head& node_head)
{
    bufferlist bl;
    uint16_t node_start = LSM_NODE_START;
    encode(node_start, bl);

    bufferlist bl_head;
    encode(node_head, bl_head);

    uint64_t encoded_len = bl_head.length();
    encode(encoded_len, bl);

    bl.claim_append(bl_head);

    int ret = cls_cxx_write2(hctx, 0, bl.length(), &bl, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
    if (ret < 0) {
        CLS_LOG(5, "ERROR: lsm_write_node: failed to write lsm node");
        return ret;
    }

    return 0;
}

/**
 * Write one full node including the node head and the node body (data entries)
 */
int lsm_write_one_node(cls_method_context_t hctx, cls_lsm_node_head& node_head, std::vector<cls_lsm_entry>& entries)
{
    for (auto entry : entries) {
        lsm_bloomfilter_insert(node_head.bloomfilter_store_ever, to_string(entry.key));
        lsm_bloomfilter_insert(node_head.bloomfilter_store, to_string(entry.key));

        auto entry_length = lsm_write_entry(hctx, entry, node_head.entry_end_offset);
        if (entry_length < 0) {
            CLS_LOG(1, "ERROR: lsm_write_one_node - failed to write entry");
            continue;
        }
        node_head.entry_end_offset += entry_length;
        node_head.size++;
    }

    // update the node_head now that all data is written
    auto ret = lsm_write_node_head(hctx, node_head);
    if (ret < 0) {
        CLS_LOG(1, "ERROR: lsm_write_one_node - failed updating node head");
        return ret;
    }

    return 0;
}

/**
 * Write with compaction
 */
int lsm_write_with_compaction(cls_method_context_t hctx, cls_lsm_node head root, std::vector<cls_lsm_entry> new_entries)
{
    // get entries for compaction
    bufferlist bl_chunk;
    auto ret = cls_cxx_read(hctx, root.entry_start_offset, (root.entry_end_offset - root.entry_start_offset), &bl_chunk);
    if (ret < 0) {
        CLS_LOG(1, "ERROR: cls_lsm_write_node: failed to read data from root");
        return ret;
    }
    std::vector<uint64_t> empty_keys;
    cls_lsm_get_entries_ret entries_ret;
    ret = lsm_get_entries(&bl_chunk, empty_keys, old_entries_ret);
    if (ret < 0) {
        CLS_LOG(1, "ERROR: cls_lsm_write_node: failed to get entries from root");
        return ret;
    }

    // split the entries
    std::map<std::string, bufferlist> tgt_objects; 
    lsm_get_scatter_targets(root, old_entries_ret.entries, tgt_objects);

    // add the new entries to the new "root"
    lsm_add_new_entries_to_root_object(root, new_entries, tgt_objects);

    // now compact
    ret = lsm_compact(hctx, tgt_objects, root.pool);
}

/**
 * Scatter data from a node when it needs to be compacted
 */
int lsm_compact(cls_method_context_t hctx, std::map<std::string, bufferlist> tgt_objects, std::string pool)
{
    int r = cls_cxx_scatter_wait_for_completions(hctx);
    if (r == -EAGAIN) {
        bufferlist in;
        r = cls_cxx_scatter(hctx, tgt_objects, pool, LSM_CLASS, LSM_WRITE_TO_INTERNAL_NODES, in);
    } else {
        if (r != 0) {
            CLS_ERR("%s: remote write failed. error=%d", __PRETTY_FUNCTION__, r);
        }
    }
    return r;
}

/**
 * Find the target object ids to scatter data into
 */
void lsm_get_scatter_targets(cls_lsm_node_head& head, std::vector<cls_lsm_entry>& entries, std::map<std::string, bufferlist>& tgt_child_objects)
{
    std::vector<std::vector<cls_lsm_entry>> entries_splits(head.key_range.splits);
    uint64_t increment = (head.key_range.high_bound - head.key_range.low_bound) / head.key_range.splits;
    for (auto entry : entries) {
        uint64_t low = head.key_range.low_bound;
        for (int i = 0; i < head.key_range.splits; i++) {
            uint64_t high = low + increment;
            if (entry.key >= low && entry.key < high) {
                entries_splits[i].push_back(entry);
                break;
            }
            low = high;
        }
    }

    for (uint64_t i=0; i < entries_splits.size(); i++) {
        for (uint64_t j = 0; j < head.column_group_splits.size(); j++) {
            bufferlist bl_data;

            bool is_root = false;
            encode(is_root, bl_data);

            cls_lsm_node_head child_head;
            std::string object_id = head.my_object_id+"/"+"kr-"+to_string(i+1)+"cg-"+to_string(j+1);
            child_head.my_object_id = object_id;
            child_head.pool = head.pool;
            child_head.my_level = head.my_level + 1;
            child_head.levels = head.levels;

            child_head.key_range.low_bound = head.key_range.low_bound + i * increment;
            child_head.key_range.high_bound = child_head.key_range.low_bound + increment;
            child_head.key_range.splits = head.key_range.splits;
        
            child_head.capacity = head.capacity;
            child_head.size = entries_splits[i].size();

            child_head.entry_start_offset = head.length() + sizeof(uint64_t) + sizeof(uint16_t) + LSM_DATA_START_PADDING;
            child_head.entry_end_offset = child_head.entry_start_offset;

            child_head.column_group_splits =
                    lsm_make_column_group_splits_for_children(column_group_splits[j], LSM_COLUMN_SPLIT_FACTOR);
            
            child_head.bloomfilter_store_ever = std::vector<bool>(BLOOM_FILTER_STORE_SIZE_256K, false);
            child_head.bloomfilter_store = std::vector<bool>(BLOOM_FILTER_STORE_SIZE_256K, false);

            encode(child_head, bl_data);

            std::vector<cls_lsm_entry> child_entries =
                    lsm_make_data_entries_for_children(entries_splits[i], column_group_splits[j]);
            encode(child_entries, bl_data);

            tgt_child_objects[child_name] = bl_data;
        }
    }
}

/**
 * Add new entries to the root object for scattering together 
 */
void lsm_add_new_entries_to_root_object(cls_lsm_node_head root, std::vector<cls_lsm_entry>& new_entries, std::map<std::string, bufferlist>& tgt_objects)
{
    bufferlist bl_data;

    bool is_root = true;
    encode(is_root, bl_data);

    root.entry_end_offset = root.entry_start_offset;
    root.size = 0;
    root.bloomfilter_store = std::vector<bool>(BLOOM_FILTER_STORE_SIZE_256K, false);
    encode(root, bl_data);
 
    encode(new_entries, bl_data);

    tgt_objects[root.my_object_id] = bl_data;
}

/**
 * Make column group splits for children from a parent's splits
 */
std::vector<std::set<std::string>> lsm_make_column_group_splits_for_children(std::set<std::string>& column_splits, int ways)
{
    std::vector<std::string> col_vec(column_splits.begin(), column_splits.end());
    int total = col_vec.size();
    int share = total / ways;
    int count = 0;
    std::set<std::string> columns;
    std::vector<std::set<std::string>> result_column_splits;

    for (auto col : col_vec) {
        count++;
        if (count < share) {
            columns.insert(col);
        } else {
            std::set<std::string> new_columns(columns);
            result_column_splits.push_back(new_columns);
            columns.clear();
            count = 0;
        }
    }
    
    return result_column_splits;
}

/**
 * Make data entries for children
 */
std::vector<cls_lsm_entry> lsm_make_data_entries_for_children(std::vector<cls_lsm_entry>& entries, std::set<std::string>& columns)
{
    std::vector<cls_lsm_entry> child_entries;
    for (auto entry : entries) {
        cls_lsm_entry child_entry;
        child_entry.key = entry.key;

        std::map<std::string, ceph::buffer::list> child_value;
        for (auto val : entry.value) {
            if (columns.find(val.first) != columns.end()) {
                child_value.insert(std::pair<std::string, ceph::buffer::list>(val.first, val.second));
            }
        }
        child_entry.value = child_value;

        child_entries.push_back(child_entry);
    }

    return child_entries;
}
