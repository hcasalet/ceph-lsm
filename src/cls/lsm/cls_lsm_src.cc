#include <algorithm>

#include "include/types.h"
#include "objclass/objclass.h"
#include "cls/lsm/cls_lsm_types.h"
#include "cls/lsm/cls_lsm_ops.h"
#include "cls/lsm/cls_lsm_bloomfilter.h"
#include "cls/lsm/cls_lsm_src.h"

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
    tree.all_columns = op.all_columns;
    tree.size = 0;
    tree.per_node_capacity = op.max_capacity;

    // total length of the tree-config = tree other fields + config_end_offset + LSM_TREE_START
    bufferlist bl;
    encode(tree, bl);
    tree.data_start_offset = bl.length() + sizeof(uint64_t) + sizeof(uint16_t) + LSM_ROOT_DATA_START_PADDING;
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
int lsm_read_node(cls_method_context_t hctx, cls_lsm_get_entries_op& op, cls_lsm_get_entries_ret& op_ret)
{
    // Reading the items from the level-0 node. lsm_read_from_root returns ret as the number of 
    // rows that exist in the system. When ret equals 0, it means the keys are not found in the
    // system. 
    auto ret = lsm_read_from_root(hctx, op, op_ret);

    if (ret < 0) {
        CLS_LOG(1, "INFO: failed to read from tree root");
        return ret;
    }

    if (ret == 0) {
        CLS_LOG(1, "INFO: keys do not exist in the system");
        return ret;
    }

    if (op_ret.entries.size() >= op.keys.size()) {
        CLS_LOG(1, "INFO: found all the keys in level-0");
        return 0;
    }

    // If we come here, it means that there are keys that are looked for existing in the system 
    // but they are in levels below level-0, so we have to look downward the tree. First step 
    // from here is to read the node head from level-1 on.
   /* cls_lsm_node_head node_head;
    ret = lsm_read_node_head(hctx, node_head);
    if (ret < 0) {
        return ret;
    }

    // Check the sub-root-node's bloom filter
    ret = lsm_check_if_key_exists(node_head.bloomfilter_store, op);
    if (ret < 0) {
        return ret;
    }
    if (op.keys.size() == 0) {
        CLS_LOG(1, "INFO: keys do not exist in the object store, returning nothing \n");
        return -ENODATA;
    }

    // get the data "rows"
    ret = lsm_get_entries(hctx, node_head, op, op_ret);
    if (ret < 0) {
        return ret;
    }*/

    return 0;
}

/**
 * When a read request comes in, it first hits the root node. Root node has two bloomfilter
 * stores: one is for the very existence of any key in the system; the other is whether one 
 * key exists in the root node. 
 */
int lsm_read_from_root(cls_method_context_t hctx, cls_lsm_get_entries_op& op, cls_lsm_get_entries_ret& op_ret)
{
    cls_lsm_tree_config tree;
    auto ret = lsm_read_tree_config(hctx, tree);
    if (ret < 0) {
        CLS_LOG(5, "ERROR: reading from tree root failed");
        return -EINVAL;
    }

    // Check if the system has the keys at all
    ret = lsm_check_if_key_exists(tree.bloomfilter_store_all, op);
    if (op.keys.size() == 0) {
        CLS_LOG(5, "INFO: lsm_read_frm_root - keys not found in the system");
        return 0;
    }

    // Read the entries to op_ret if they exist in root node
    ret = lsm_get_entries(hctx, tree, op, op_ret);
    if (ret < 0) {
        CLS_LOG(5, "INFO: lsm_read_from_root - getting entries failed");
        return ret;
    }

    return op_ret.entries.size();
}

/**
 * read header of nodes starting from level-0
 */
int lsm_read_node_head(cls_method_context_t hctx, cls_lsm_node_head& node_head)
{
   /* uint64_t read_size = CHUNK_SIZE, start_offset = 0;

    bufferlist bl_node;
    const auto ret = cls_cxx_read(hctx, start_offset, read_size, &bl_node);
    if (ret < 0) {
        CLS_LOG(5, "ERROR: lsm_read_node: failed read lsm node");
        return ret;
    }
    if (ret == 0) {
        CLS_LOG(20, "INFO: lsm_read_node: empty node, not initialized yet");
        return -EINVAL;
    }

    // Process the chunk of data read
    auto it = bl_node.cbegin();
    
    // Check node start
    uint16_t node_start;
    try {
        decode(node_start, it);
    } catch (const ceph::buffer::error& err) {
        CLS_LOG(0, "ERROR: lsm_read_node: failed to decode node start");
        return -EINVAL;
    }
    if (node_start != LSM_NODE_START) {
        CLS_LOG(0, "ERROR: lsm_read_node: invalid node start");
        return -EINVAL;
    }

    uint64_t encoded_len;
    try {
        decode(encoded_len, it);
    } catch (const ceph::buffer::error& err) {
        CLS_LOG(0, "ERROR: lsm_read_node: failed to decode encoded size: %s", err.what());
        return -EINVAL;
    }

    if (encoded_len > (read_size - LSM_NODE_OVERHEAD)) {
        start_offset = read_size;
        read_size = encoded_len - (read_size - LSM_NODE_OVERHEAD);
        bufferlist bl_remaining_node;
        const auto ret = cls_cxx_read2(hctx, start_offset, read_size, &bl_remaining_node, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
        if (ret < 0) {
            CLS_LOG(5, "ERROR: lsm_read_node: failed to read the remaining part of the node");
            return ret;
        }
        bl_node.claim_append(bl_remaining_node);
    }

    try {
        decode(node_head, it);
    } catch (const ceph::buffer::error& err) {
        CLS_LOG(0, "ERROR: lsm_read_node: failed to decode node: %s", err.what());
        return -EINVAL;
    }*/

    return 0;
}

int lsm_check_if_key_exists(std::vector<bool> bloomfilter_store, cls_lsm_get_entries_op& op)
{
    std::vector<uint64_t> op_keys;
    for (auto key : op.keys) {
        if (lsm_bloomfilter_contains(bloomfilter_store, to_string(key))) {
            op_keys.push_back(key);
        }
    }
    op.keys.clear();
    op.keys.insert(op.keys.end(), op_keys.begin(), op_keys.end());

    return 0;
}

int lsm_get_entries(cls_method_context_t hctx, cls_lsm_tree_config& tree, cls_lsm_get_entries_op& op, cls_lsm_get_entries_ret& op_ret)
{
    if (tree.data_start_offset == tree.data_end_offset) {
        CLS_LOG(20, "INFO: lsm_get_entries: node is empty, offset is %lu", tree.data_end_offset);
        return 0;
    }

    bufferlist bl_chunk;
    auto ret = cls_cxx_read(hctx, tree.data_start_offset, (tree.data_end_offset - tree.data_start_offset), &bl_chunk);
    if (ret < 0) {
        return ret;
    }

    uint16_t entry_start = 0;
    uint64_t data_size = 0;
    uint64_t size_to_process = bl_chunk.length();
    unsigned index = 0;
    auto it = bl_chunk.cbegin();

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
        index += sizeof(uint16_t);
        size_to_process -= sizeof(uint16_t);

        try {
            decode(data_size, it);
        } catch (const ceph::buffer::error& err) {
            CLS_LOG(10, "ERROR: lsm_get_entries: failed to decode data size: %s", err.what());
            return -EINVAL;
        }
        CLS_LOG(10, "INFO: lsm_get_entries: data size %lu", data_size);
        index += sizeof(uint64_t);
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

        // check if this is the entry wanted
        auto it = std::find(op.keys.begin(), op.keys.end(), entry.key);
        if (it != op.keys.end()) {
            op_ret.entries.emplace_back(entry);
            op.keys.erase(it);
        }

        // if we have found all we need, break
        if (op.keys.empty()) {
            break;
        }
    } while (index < bl_chunk.length());

    // we have gone through all data in the node but might need to ask from our children
    if (!op.keys.empty()) {
        /*cls_lsm_get_child_object_name_ret child_ret;
        lsm_get_child_object_names(head, op, child_ret);

        bufferlist keys_bl;
        encode(op.keys, keys_bl);
        cls_cxx_gather(hctx, child_ret.child_object_name, head.pool, "cls_lsm", "cls_lsm_read_node", keys_bl);*/
    }
    
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

    if (tree_config.size + op.bl_data_map.size() <= tree_config.per_node_capacity) {
        // there is enough space for the data so we will start the loop to write it in
        for (auto entry : op.bl_data_map) {
            ret = lsm_write_root_node(hctx, tree_config, entry.first, entry.second);
            if (ret < 0) {
                CLS_LOG(1, "ERROR: lsm_write_data - failed to write data");
                return ret;
            }
        }

        // update the tree_config now that all data is written
        lsm_write_tree_config(hctx, tree_config);
    } else {
        // compaction
    }

    return 0;
}

int lsm_write_root_node(cls_method_context_t hctx, cls_lsm_tree_config& tree_config, uint64_t key, bufferlist bl_data)
{
    // first step is to write the bloomfilter stores
    lsm_bloomfilter_insert(tree_config.bloomfilter_store_all, to_string(key));
    lsm_bloomfilter_insert(tree_config.bloomfilter_store_root, to_string(key));

    // then we encode the data with start marker and its length
    bufferlist bl;
    uint16_t entry_start = LSM_ENTRY_START;
    encode(entry_start, bl);

    uint64_t encoded_len = bl_data.length();
    encode(encoded_len, bl);

    bl.claim_append(bl_data);

    // then we will write the data into the object
    auto ret = cls_cxx_write2(hctx, tree_config.data_end_offset, bl.length(), &bl, CEPH_OSD_OP_FLAG_FADVISE_WILLNEED);
    CLS_LOG(1, "finished writing data into the node");
    if (ret < 0) {
        CLS_LOG(1, "ERROR: lsm_write_root_node: failed to write lsm root node");
        return ret;
    }

    tree_config.data_end_offset += bl.length();
    tree_config.size++;

    return 0;
}

int lsm_write_node_head(cls_method_context_t hctx, cls_lsm_node_head& node_head)
{
    /*bufferlist bl;
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
    }*/

    return 0;
}

/**
 * method to be called by scatter in lsm_init. Used to init level 1 nodes
 */
int lsm_init_node(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    /*auto in_iter = in->cbegin();
    cls_lsm_node_head head;
    try {
        decode(head, in_iter);
    } catch (ceph::buffer::error& err) {
        CLS_LOG(5, "ERROR: lsm_init_node_head failed to decode head");
        return -EINVAL;
    }

    return lsm_write_node_head(hctx, head);*/

    return 0;
}

int lsm_append_entries(cls_method_context_t hctx, cls_lsm_append_entries_op& op, cls_lsm_node_head& head)
{
    // capacity checking
    /*if (head.size + op.bl_data_vec.size() > head.max_capacity) {
        CLS_LOG(0, "ERROR: No space left in the node, need compaction");
        return -ENOSPC;
    }

    for (auto& bl_data : op.bl_data_vec) {
        bufferlist bl;
        uint16_t entry_start = LSM_ENTRY_START;
        encode(entry_start, bl);
        bufferlist bl_data_entry;
        encode(bl_data, bl_data_entry);
        uint64_t data_size = bl_data_entry.length();
        encode(data_size, bl);
        bl.claim_append(bl_data_entry);

        CLS_LOG(10, "INFO: lsm_append_entries: total entry size to be written is %u and data size is %lu", bl.length(), data_size);

        auto ret = cls_cxx_write2(hctx, head.entry_end_offset, bl.length(), &bl, CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL);
        if (ret < 0) {
            return ret;
        }

        head.entry_end_offset += bl.length();
        head.size++; 
    }

    return lsm_write_node_head(hctx, head);*/
    return 0;
}

int lsm_compact_node(cls_method_context_t hctx, cls_lsm_append_entries_op& op, cls_lsm_node_head& head)
{
    /*auto ret = cls_cxx_scatter_wait_for_completions(hctx);
    if (ret == -EAGAIN) {
        std::map<std::string, cls_lsm_append_entries_op> tgt_objs;
        std::vector<cls_lsm_entry> all_src;
        all_src.reserve(op.bl_data_vec.size() + head.size);
        cls_lsm_get_entries_op entries_op;
        cls_lsm_get_entries_ret entries_ret;
        ret = lsm_get_entries(hctx, head, entries_op, entries_ret);
        all_src.insert(all_src.end(), op.bl_data_vec.begin(), op.bl_data_vec.end());
        all_src.insert(all_src.end(), entries_ret.entries.begin(), entries_ret.entries.end());

        uint64_t key_lb = head.key_range.low_bound;
        uint64_t key_hb = head.key_range.high_bound;
        uint64_t key_increment = (key_hb - key_lb)/head.key_range_splits + 1;
        uint64_t cg_size = head.column_group_splits.size();

        // initialize target objects with the keys
        for (uint32_t i = 0; i < head.key_range_splits; i++) {
            std::stringstream ss;
            ss << "/lv-" << (head.level + 1) << "/kr-" << i;
            for (uint32_t j = 0; j < cg_size; j++) {
                ss << "/cg-" << j;
                tgt_objs.insert(std::pair<std::string, cls_lsm_append_entries_op>(ss.str(), cls_lsm_append_entries_op()));
            }
        }

        // populate target objects with the values
        for (auto src : all_src) {
            uint64_t lowbound = key_lb;
            for (uint32_t i = 0; i < head.key_range_splits; i++) {
                if (src.key >= lowbound && src.key < lowbound + key_increment) {
                    std::vector<cls_lsm_entry> split_data;
                    split_data.reserve(cg_size);

                    // every split group has the same key
                    for (uint32_t j = 0; j < cg_size; j++) {
                        split_data[j].key = src.key;
                    }

                    // split value ("columns")
                    for (auto it = src.value.begin(); it != src.value.end(); it++) {
                        for (uint32_t j = 0; j < cg_size; j++) {
                            if (head.column_group_splits[j].find(it->first) != head.column_group_splits[j].end()) {
                                split_data[j].value.insert(std::pair<std::string, ceph::buffer::list>(it->first, it->second));
                            }
                        }
                    }

                    // place the groups in target objects
                    for (uint32_t j = 0; j < cg_size; j++) {
                        std::stringstream ss;
                        ss << "/lv-" << (head.level + 1) << "/kr-" << i << "/cg-" << j;
                        bufferlist spbl;
                        encode(split_data[j], spbl);
                        tgt_objs[ss.str()].bl_data_vec.push_back(spbl);
                    }
                    break;
                }
                lowbound += key_increment;
            }
        }

        // remote write to child objects
        bufferlist *in; 
        std::map<std::string, bufferlist> target_objects;
        for (auto tgt_obj : tgt_objs) {
            bufferlist bl_obj;
            encode(tgt_obj.second, bl_obj);
            target_objects.insert(std::pair<std::string, bufferlist>(tgt_obj.first, bl_obj));
        }

        ret = cls_cxx_scatter(hctx, target_objects, head.pool, "cls_lsm", "cls_lsm_write_node", *in);
    } else {
        if (ret != 0) {
            CLS_ERR("%s: compaction failed. error=%d", __PRETTY_FUNCTION__, ret);
        }
    }*/

    return 0; 
}

int lsm_get_child_object_names(cls_lsm_node_head& head, cls_lsm_get_entries_op& op, cls_lsm_get_child_object_name_ret& op_ret)
{
    // key ranges that are involved
    /*std::map<int, std::set<uint64_t>> range_keys;
    uint64_t key_increment = (head.key_range.high_bound - head.key_range.low_bound) / head.key_range_splits;
    for (auto key : op.keys) {
        uint64_t low = head.key_range.low_bound;

        for (uint64_t i=0; i < head.key_range_splits; i++) {
            uint64_t high = low + key_increment;
            if (key >= low && key < high) {
                if (range_keys.find(i) != range_keys.end()) {
                    range_keys.find(i)->second.insert(key);
                } else {
                    std::set<uint64_t> keys_to_read;
                    keys_to_read.insert(key);
                    range_keys.insert(std::pair<int, std::set<uint64_t>>(i, keys_to_read));
                }
                break;
            }
            low = high;
        }
    }

    // column groups that need to be read from
    std::set<int> clm_group_ids;
    for (auto clm : op.columns) {
        for (uint64_t i=0; i < head.column_group_splits.size(); i++) {
            if (head.column_group_splits[i].find(clm) != head.column_group_splits[i].end()) {
                clm_group_ids.insert(i);
                break;
            }

            if (i == head.column_group_splits.size()-1) {
                CLS_LOG(10, "ERROR: invalid column name %s", clm.c_str());
            }
        }
    }

    // construct child object names
    std::set<std::string> children_objects;
    for (auto range_key : range_keys) {
        for (auto clm_group_id : clm_group_ids) {
            std::stringstream ss;
            ss << "/parent_id:" << head.my_object_id;
            ss << "/lv" << std::to_string(head.level+1) << "-kr" << std::to_string(range_key.first) << "-cg" << std::to_string(clm_group_id);

            children_objects.insert(ss.str());
        }
    }

    op_ret.child_object_name = children_objects;

    CLS_LOG(20, "INFO: lsm_get_child_object_names: size of child objects: %lu", op_ret.child_object_name.size());
    */
    return 0;
}

/*
    std::map<std::string, bufferlist> level1_objects;
    for (int i = 0; i < fan_out; i++) {
        cls_lsm_node_head head;
        head.pool = op.tree_name;
        head.my_object_id = tree.my_object_id + "/lv1-kr" + std::to_string(i+1);
        head.level = 1;

        uint64_t key_increment = (op.key_range.high_bound - op.key_range.low_bound)/fan_out;
        head.key_range.low_bound = op.key_range.low_bound + key_increment * i;
        if (i == fan_out-1) {
            head.key_range.high_bound = op.key_range.high_bound;
        } else {
            head.key_range.high_bound = op.key_range.low_bound + key_increment * (i+1);
        }

        head.max_capacity = op.max_capacity;
        head.size = 0;
        head.entry_start_offset = LSM_NON_ROOT_DATA_START_100K;
        head.entry_end_offset = LSM_NON_ROOT_DATA_START_100K;

        // key ranges can only split fan_out/2 ways, because the other half is for splitting columns
        head.key_range_splits = fan_out/2;

        // splitting columns
        std::vector<std::set<std::string>> column_splits(2);

        uint64_t num_cols = op.all_columns.size();
        uint64_t col=0;
        for (auto itr = op.all_columns.begin(); itr != op.all_columns.end(); itr++) {
            if (col < num_cols/2) {
                column_splits[0].insert(*itr);
            } else {
                column_splits[1].insert(*itr);
            }
            col++;
        }
        head.column_group_splits = column_splits;

        head.bloomfilter_store.reserve(BLOOM_FILTER_STORE_SIZE_64K);
        for (int i = 0; i < BLOOM_FILTER_STORE_SIZE_64K; i++) {
            head.bloomfilter_store.push_back(false);
        }

        bufferlist bl_node_head;
        encode(head, bl_node_head);
        level1_objects[head.my_object_id] = bl_node_head;
    }

    CLS_LOG(20, "INFO: lsm_init node with %u level 1 nodes", fan_out);

    bufferlist *in;
    ret = cls_cxx_scatter(hctx, level1_objects, op.tree_name, "cls_lsm", "lsm_write_node_head", *in);
    */