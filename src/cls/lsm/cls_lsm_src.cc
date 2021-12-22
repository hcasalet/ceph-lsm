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
    cls_lsm_tree_config tree;
    auto ret = lsm_read_tree_config(hctx, tree);
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
    tree.pool = op.pool_name;
    tree.tree_name = op.tree_name;
    tree.levels = op.levels;
    tree.key_range = op.key_range;
    tree.all_column_splits = op.all_column_splits;
    tree.size = 0;
    tree.per_node_capacity = op.max_capacity;
    tree.bloomfilter_store_all = std::vector<bool>(BLOOM_FILTER_STORE_SIZE_256K, false);
    tree.bloomfilter_store_root = std::vector<bool>(BLOOM_FILTER_STORE_SIZE_256K, false);

    // total length of the tree-config = tree other fields + config_end_offset + LSM_TREE_START
    bufferlist bl;
    encode(tree, bl);
    tree.data_start_offset = bl.length() + sizeof(uint64_t) * 3 + sizeof(uint16_t) + LSM_DATA_START_PADDING;
    tree.data_end_offset = tree.data_start_offset;

    // write out the tree-config
    ret = lsm_write_tree_config(hctx, tree);
    if (ret < 0) {
        CLS_LOG(5, "ERROR: failed to initialize lsm tree");
        return ret;
    }

    return 0;
}

/**
 * read in the tree info (read from level-0 root node)
 */
int lsm_read_tree_config(cls_method_context_t hctx, cls_lsm_tree_config& tree)
{
    uint64_t read_size = CHUNK_SIZE, start_offset = 0;

    bufferlist bl_tree;
    auto ret = cls_cxx_read(hctx, start_offset, read_size, &bl_tree);

    if (ret < 0) {
        CLS_LOG(5, "ERROR: lsm_read_tree_config: failed read lsm tree config");
        return ret;
    }
    if (ret == 0) {
        CLS_LOG(20, "INFO: lsm_read_tree_config: tree not initialized yet");
        return -EINVAL;
    }

    // Process the chunk of data read
    auto it = bl_tree.cbegin();

    // check tree start
    uint16_t tree_start;
    try {
        decode(tree_start, it);
    } catch (const ceph::buffer::error& err) {
        CLS_LOG(0, "ERROR: lsm_read_tree_config: failed to decode tree start");
        return -EINVAL;
    }
    if (tree_start != LSM_TREE_START) {
        CLS_LOG(0, "ERROR: lsm_read_tree_config: invalid tree start");
        return -EINVAL;
    }
    // check tree length
    uint64_t tree_length;
    try {
        decode(tree_length, it);
    } catch (const ceph::buffer::error& err) {
        CLS_LOG(0, "ERROR: lsm_read_tree_config: failed to decode tree length");
        return -EINVAL;
    }

    // if read-in length is not covering the whole tree, need to read more
    if (read_size - sizeof(uint16_t) - sizeof(uint64_t) < tree_length) {
        CLS_LOG(1, "need to read again!");
        uint64_t continue_to_read = tree_length - read_size + sizeof(uint16_t) + sizeof(uint64_t);
        start_offset += read_size;
        bufferlist bl_tree_2;
        ret = cls_cxx_read(hctx, start_offset, continue_to_read, &bl_tree_2);

        if (ret < 0) {
            CLS_LOG(0, "ERROR: lsm_read_tree_config: failed to read the rest of the tree");
            return ret;
        }
        bl_tree.claim_append(bl_tree_2);
    }

    // get tree config
    try {
        decode(tree, it);
    } catch (const ceph::buffer::error& err) {
        CLS_LOG(0, "ERROR: lsm_read_node: failed to decode node: %s", err.what());
        return -EINVAL;
    }

    return 0;
}

/**
 * write the tree root node (level-0 node)
 */
int lsm_write_tree_config(cls_method_context_t hctx, cls_lsm_tree_config& tree)
{
    bufferlist bl;
    uint16_t tree_start = LSM_TREE_START;
    encode(tree_start, bl);

    bufferlist bl_data;
    encode(tree, bl_data);
    uint64_t encoded_length = bl_data.length();
    encode(encoded_length, bl);

    bl.claim_append(bl_data);

    int ret = cls_cxx_write2(hctx, 0, bl.length(), &bl, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
    if (ret < 0) {
        CLS_LOG(5, "ERROR: lsm_write_tree_config: failed to write lsm tree");
        return ret;
    }

    return 0;
}

/**
 * Read rows whose key matching "keys" (returning only asked columns)
 */
int lsm_read_data(cls_method_context_t hctx, cls_lsm_get_entries_op& op, cls_lsm_get_entries_ret& op_ret, std::map<std::string, std::vector<uint64_t>> src_objs_map)
{
    // Reading the items from the level-0 node. lsm_read_from_root returns ret as the number of 
    // rows that exist in the system. When ret equals 0, it means the keys are not found in the
    // system.
    cls_lsm_tree_config tree;
    std::vector<uint64_t> found_keys_all;
    auto ret = lsm_read_from_root(hctx, tree, op, op_ret, found_keys_all);
    CLS_LOG(1, "INFO: found %lu keys, read %lu keys in root, %lu more keys to read", found_keys_all.size(), op_ret.entries.size(), op.keys.size());

    if (ret < 0) {
        CLS_LOG(1, "INFO: failed to read from tree root");
        return ret;
    }

    if (found_keys_all.size() == 0) {
        CLS_LOG(1, "keys not found in the system");
        return -ENODATA;
    }

    // every key in op was removed when it was found in lsm_read_from_root()
    if (op.keys.size() == 0) {
        CLS_LOG(1, "INFO: found all the keys in level-0 root object");
        return 0;
    }

    // If we come here, it means that there are still keys that exist below level 0 root 
    // object, so we have to keep looking for them
    ret = lsm_read_from_below_level_0(hctx, tree.key_range, tree.all_column_splits, tree.tree_name, tree.pool, op, op_ret, src_objs_map);
    if (ret < 0) {
        return ret;
    }
    
    return 0;
}

/**
 * When a read request comes in, it first hits the root node. Root node has two bloomfilter
 * stores: one is for the very existence of any key in the system; the other is whether one 
 * key exists in the root node. 
 */
int lsm_read_from_root(cls_method_context_t hctx, cls_lsm_tree_config& tree, cls_lsm_get_entries_op& op, cls_lsm_get_entries_ret& op_ret, std::vector<uint64_t>& found_keys_all)
{
    auto ret = lsm_read_tree_config(hctx, tree);
    if (ret < 0) {
        CLS_LOG(5, "ERROR: reading from tree root failed");
        return -EINVAL;
    }

    // Check if the system has the keys at all
    ret = lsm_check_if_key_exists(tree.bloomfilter_store_all, op.keys, found_keys_all);
    if (found_keys_all.size() == 0) {
        CLS_LOG(5, "INFO: lsm_read_frm_root - keys not found in the system");
        return 0;
    }

    // Get the items that exist in the root
    std::vector<uint64_t> found_keys_root;
    ret = lsm_check_if_key_exists(tree.bloomfilter_store_root, op.keys, found_keys_root);

    // Get the remaining keys to look for
    op.keys.clear();
    op.keys.insert(op.keys.begin(), found_keys_all.begin(), found_keys_all.end());
    for (auto key : found_keys_root) {
        op.keys.erase(std::remove(op.keys.begin(), op.keys.end(), key), op.keys.end());
    }

    // Read the entries to op_ret if they exist in root node
    bufferlist bl_chunk;
    ret = cls_cxx_read(hctx, tree.data_start_offset, (tree.data_end_offset - tree.data_start_offset), &bl_chunk);
    ret = lsm_read_entries_from_root(hctx, &bl_chunk, found_keys_root, op_ret);
    if (ret < 0) {
        CLS_LOG(5, "INFO: lsm_read_from_root - getting entries failed");
        return ret;
    }

    return op.keys.size();
}

/**
 * Invoked by lsm_read_from_root, this function gets the entries
 */
int lsm_read_entries_from_root(cls_method_context_t hctx, bufferlist *bl_chunk, std::vector<uint64_t>& found_keys, cls_lsm_get_entries_ret& op_ret)
{
    uint64_t size_to_process = bl_chunk->length();
    if (size_to_process <= 0) {
        CLS_LOG(1, "INFO: no data entries in object");
    } else {
        auto ret = lsm_get_entries(bl_chunk, found_keys, op_ret);
        if (ret < 0) {
            return ret;
        }
    }
    
    return 0;
}

/**
 * read from object below level 0 (non-root objects)
 */
int lsm_read_from_below_level_0(cls_method_context_t hctx, cls_lsm_key_range& key_range, std::vector<std::set<std::string>> column_splits,
                                std::string parent_id, std::string pool, cls_lsm_get_entries_op& op, cls_lsm_get_entries_ret& op_ret,
                                std::map<std::string, std::vector<uint64_t>> src_objs_map)
{
    uint64_t lowbound = key_range.low_bound;
    uint64_t highbound = key_range.high_bound;
    uint64_t increment = (key_range.high_bound - key_range.low_bound) / key_range.splits + 1;

    std::vector<std::vector<uint64_t>> key_groups(key_range.splits);
    for (auto key : op.keys) {
        if (key < lowbound || key > highbound) {
            CLS_LOG(1, "key is out of range: %lu", key);
            continue;
        } 

        uint64_t ind = (key - lowbound) / increment;
        key_groups[ind].push_back(key);
    }

    std::set<uint16_t> col_grps;
    lsm_get_column_groups(op.columns, column_splits, col_grps);
    for (uint64_t i = 0; i < key_groups.size(); i++) {
        if (key_groups[i].size() > 0) {
            for (auto col_grp : col_grps) {
                std::string child_name = parent_id+"/"+"kr-"+to_string(i+1)+":cg-"+to_string(col_grp);
                src_objs_map[child_name] = key_groups[i];
            }
        }
    }
    
    return 0;
}

/**
 * Get Child object ids
 */
int lsm_get_child_object_ids(cls_lsm_node_head head, std::vector<uint64_t> keys, std::vector<std::string>& cols, std::map<std::string, std::vector<uint64_t>> src_objs_map)
{
    std::set<uint16_t> col_grps
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
                src_objs_map[object_id] = key_groups[i];
            }  
        }
    }
}

/**
 * Read node head
 */
int lsm_read_node_head(cls_method_context_t hctx, cls_lsm_node_head& node_head)
{
    uint64_t read_size = CHUNK_SIZE, start_offset = 0;

    bufferlist bl_head;
    const auto ret = cls_cxx_read(hctx, start_offset, read_size, &bl_head);
    CLS_LOG(1, "santa cruz read node head %u", ret);
    if (ret < 0) {
        CLS_LOG(1, "ERROR: lsm_read_node_head: failed read lsm node head");
        return ret;
    }
    if (ret == 0) {
        CLS_LOG(1, "INFO: lsm_read_node_head: empty node, not initialized yet");
        return -EINVAL;
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
int lsm_check_if_key_exists(std::vector<bool> bloomfilter_store, std::vector<uint64_t>& search_keys, std::vector<uint64_t>& found_keys)
{
    for (auto key : search_keys) {
        if (lsm_bloomfilter_contains(bloomfilter_store, to_string(key))) {
            found_keys.push_back(key);
        }
    }

    return 0;
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
 * Write the data into the root node
 */
int lsm_write_data(cls_method_context_t hctx, cls_lsm_append_entries_op& op)
{
    // Read the tree-config to know if the root node has capacity
    cls_lsm_tree_config tree_config;
    auto ret = lsm_read_tree_config(hctx, tree_config);

    if (tree_config.size + op.entries.size() <= tree_config.per_node_capacity) {
        // there is enough space for the data so we will start the loop to write it in
        for (auto entry : op.entries) {
            ret = lsm_write_root_node(hctx, tree_config, entry);
            if (ret < 0) {
                CLS_LOG(1, "ERROR: lsm_write_data - failed to write data");
                return ret;
            }
        }

    } else {
        // first read in what the root node has
        bufferlist bl_chunk;
        auto ret = cls_cxx_read(hctx, tree_config.data_start_offset, (tree_config.data_end_offset - tree_config.data_start_offset), &bl_chunk);

        cls_lsm_get_entries_ret op_ret;
        std::vector<uint64_t> found_keys_all;

        ret = lsm_read_entries_from_root(hctx, &bl_chunk, found_keys_all, op_ret);

        std::vector<cls_lsm_entry> entries;
        entries.reserve(op_ret.entries.size() + op.entries.size());
        entries.insert(entries.end(), op_ret.entries.begin(), op_ret.entries.end());
        entries.insert(entries.end(), op.entries.begin(), op.entries.end());

        // split the entries for level-1 objects
        std::vector<std::vector<cls_lsm_entry>> entries_splits(tree_config.key_range.splits);
        uint64_t increment = (tree_config.key_range.high_bound - tree_config.key_range.low_bound) / tree_config.key_range.splits + 1;
        for (auto entry : entries) {
            // update the bloomfilter store in level 0 so the key is kept in the system
            lsm_bloomfilter_insert(tree_config.bloomfilter_store_all, to_string(entry.key));

            uint64_t low = tree_config.key_range.low_bound;
            for (int i = 0; i < tree_config.key_range.splits; i++) {
                uint64_t high = low + increment;
                if (entry.key >= low && entry.key < high) {
                    entries_splits[i].push_back(entry);
                    break;
                }
                low = high;
            }
        }

        // compacting to level-1 objects. Level-1 is pure row based
        std::map<std::string, bufferlist> tgt_child_objects;
        for (uint64_t i=0; i < entries_splits.size(); i++) {
            if (entries_splits[i].size() > 0) {
                for (uint64_t j = 0; j < tree_config.all_column_splits.size(); j++) {
                    std::string child_name = tree_config.tree_name+"/"+"kr-"+to_string(i+1)+":cg-"+to_string(j+1);
                    bufferlist bl_data;
                    encode(child_name, bl_data);

                    uint64_t child_level = 1;
                    encode(child_level, bl_data);

                    cls_lsm_key_range child_key_range;
                    child_key_range.low_bound = tree_config.key_range.low_bound + i * increment;
                    child_key_range.high_bound = child_key_range.low_bound + increment;
                    child_key_range.splits = tree_config.key_range.splits;
                    encode(child_key_range, bl_data);

                    uint64_t node_capacity = tree_config.per_node_capacity;
                    encode(node_capacity, bl_data);

                    std::vector<std::set<std::string>> child_column_group_splits =
                            lsm_make_column_group_splits_for_children(tree_config.all_column_splits[j], LSM_COLUMN_SPLIT_FACTOR);
                    encode(child_column_group_splits, bl_data);

                    std::vector<cls_lsm_entry> child_entries =
                            lsm_make_data_entries_for_children(entries_splits[i], tree_config.all_column_splits[j]);
                    encode(child_entries, bl_data);

                    tgt_child_objects[child_name] = bl_data;
                }
            }
        }
        ret = cls_cxx_scatter(hctx, tgt_child_objects, tree_config.pool, LSM_CLASS, LSM_WRITE_TO_INTERNAL_NODES, bl_chunk);

        ret = cls_cxx_scatter_wait_for_completions(hctx);
        if (ret != 0) {
            CLS_LOG(1, "ERROR: lsm_write_data - failed to remote write");
            return ret;
        }

    }

    // update the tree_config now that all data is written
    lsm_write_tree_config(hctx, tree_config);

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
 * Function to write data into root object
 */
int lsm_write_root_node(cls_method_context_t hctx, cls_lsm_tree_config& tree_config, cls_lsm_entry entry)
{
    // first step is to write the bloomfilter stores
    lsm_bloomfilter_insert(tree_config.bloomfilter_store_all, to_string(entry.key));
    lsm_bloomfilter_insert(tree_config.bloomfilter_store_root, to_string(entry.key));

    // then we encode the data with start marker and its length
    auto entry_length = lsm_write_entry(hctx, entry, tree_config.data_end_offset);
    if (entry_length < 0) {
        return entry_length;
    }

    tree_config.data_end_offset += entry_length;
    tree_config.size++;

    return 0;
}

/**
 * Compact data from an object to its children nodes
 */
int lsm_compact_node(cls_method_context_t hctx, std::vector<cls_lsm_entry>& entries, cls_lsm_node_head& head)
{
    // split the entries for level-1 objects
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

    std::map<std::string, bufferlist> tgt_child_objects;
    for (uint64_t i=0; i < entries_splits.size(); i++) {
        for (uint64_t j = 0; j < head.column_group_splits.size(); j++) {
            std::string child_name = head.my_object_id+"/"+"kr-"+to_string(i+1)+"cg-"+to_string(j+1);
            bufferlist bl_data;
            encode(child_name, bl_data);

            uint64_t child_level = head.my_level + 1;
            encode(child_level, bl_data);

            cls_lsm_key_range child_key_range;
            child_key_range.low_bound = head.key_range.low_bound + i * increment;
            child_key_range.high_bound = child_key_range.low_bound + increment;
            child_key_range.splits = head.key_range.splits;
            encode(child_key_range, bl_data);

            uint64_t node_capacity = head.max_capacity;
            encode(node_capacity, bl_data);

            std::vector<std::set<std::string>> child_column_group_splits =
                    lsm_make_column_group_splits_for_children(head.column_group_splits[j], LSM_COLUMN_SPLIT_FACTOR);
            encode(child_column_group_splits, bl_data);

            std::vector<cls_lsm_entry> child_entries =
                    lsm_make_data_entries_for_children(entries_splits[i], head.column_group_splits[j]);
            encode(child_entries, bl_data);
            tgt_child_objects[child_name] = bl_data;
        }
    }

    bufferlist bl_chunk; 
    auto ret = cls_cxx_scatter(hctx, tgt_child_objects, head.pool, LSM_CLASS, LSM_WRITE_TO_INTERNAL_NODES, bl_chunk);

    ret = cls_cxx_scatter_wait_for_completions(hctx);
    if (ret != 0) {
        CLS_LOG(1, "ERROR: lsm_write_data - failed to remote write");
        return ret;
    }

    return 0; 
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
std::vector<cls_lsm_entry> lsm_make_data_entries_for_children(std::vector<cls_lsm_entry> entries, std::set<std::string> columns)
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
