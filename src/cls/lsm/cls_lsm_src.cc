#include <algorithm>
#include <unistd.h>

#include "include/types.h"
#include "objclass/objclass.h"
#include "cls/lsm/cls_lsm_types.h"
#include "cls/lsm/cls_lsm_ops.h"
#include "cls/lsm/cls_lsm_src.h"
#include "cls/lsm/cls_lsm_const.h"
#include "cls/lsm/cls_lsm_util.h"

using ceph::bufferlist;
using ceph::decode;
using ceph::encode;

/*
 * initializes only the root node (total == fan_out)
 */
int lsm_init(cls_method_context_t hctx, const cls_lsm_init_op& op)
{
    // check if the tree was already initialized
    cls_lsm_node_head head;
    auto ret = lsm_read_node_head(hctx, head);
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
    head.pool = op.pool_name;
    head.object_id = op.obj_name;
    head.key_range = op.key_range;
    head.size = 0;
    head.key_map = std::map<uint64_t, std::pair<uint64_t, uint64_t>>();
    head.data_start_offset = LSM_DATA_START_PADDING;
    head.data_end_offset = head.data_start_offset;

    bufferlist bl;
    encode(head, bl);

    // write out the tree-config
    ret = lsm_write_node_head(hctx, head);
    if (ret < 0) {
        CLS_LOG(5, "ERROR: failed to initialize lsm tree");
        return ret;
    }

    return 0;
}

/**
 * Read rows whose key matching "keys" (returning only asked columns)
 */
int lsm_read_data(cls_method_context_t hctx, uint64_t key, cls_lsm_entry& entry)
{
    cls_lsm_node_head head;
    auto ret = lsm_read_node_head(hctx, head);
    if (ret < 0) {
        CLS_LOG(1, "In lsm_read_data: reading node head failed");
        return ret;
    }

    uint64_t entry_start_offset;
    uint64_t entry_end_offset;
    try {
        entry_start_offset = head.key_map.at(key).first;
        entry_end_offset = head.key_map.at(key).second;
    } catch (std::out_of_range& err) {
        CLS_LOG(1, "In lsm_read_data: key does not exist: %s", err.what());
        return -EINVAL;
    }

    bufferlist bl_chunk;
    ret = cls_cxx_read(hctx, entry_start_offset, entry_end_offset, &bl_chunk);
    if (ret < 0) {
        CLS_LOG(1, "ERROR: in lsm_read_data: reading entry failed");
        return ret;
    }

    auto it = bl_chunk.cbegin();
    try {
        decode(entry, it);
    } catch (ceph::buffer::error& err) {
        CLS_LOG(1, "ERROR: in lsm_read_data: failed to decode entry \n");
        return -EINVAL;
    }
    
    return 0;
}

/**
 * Read all data in a node
 */
int lsm_readall_in_node(cls_method_context_t hctx, std::vector<cls_lsm_entry>& entries)
{
    entries.clear();

    cls_lsm_node_head head;
    auto ret = lsm_read_node_head(hctx, head);
    if (ret < 0) {
        CLS_ERR("%s: reading node head failed", __PRETTY_FUNCTION__);
        return ret;
    }

    bufferlist bl_chunk;
    ret = cls_cxx_read(hctx, head.data_start_offset, head.data_end_offset, &bl_chunk);

    auto it = bl_chunk.cbegin();
    uint64_t size_to_read = bl_chunk.length();

    do {
        uint64_t data_size = 0;
        try {
            decode(data_size, it);
        } catch (ceph::buffer::error& err) {
            CLS_ERR("%s: error decoding data size", __PRETTY_FUNCTION__);
        }

        size_to_read -= sizeof(uint64_t);
        if (data_size > size_to_read) {
            CLS_ERR("%s: not enough data to read, breaking the loop...", __PRETTY_FUNCTION__);
            break;
        }

        cls_lsm_entry entry;
        try {
            decode(entry, it);
        } catch (ceph::buffer::error& err) {
            CLS_ERR("%s: error decoding entry", __PRETTY_FUNCTION__);
        }

        entries.push_back(entry);
    } while (size_to_read > 0);

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
        const auto ret = cls_cxx_read(hctx, start_offset, read_size, &bl_remaining_head);
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
 * Read all entries from object
 */
int lsm_get_entries(bufferlist *in, std::vector<cls_lsm_entry>& entries)
{
    auto it = in->cbegin();
    uint64_t size_to_process = in->length();

    do {
        uint16_t entry_start;
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

        uint64_t data_size = 0;
        try {
            decode(data_size, it);
        } catch (const ceph::buffer::error& err) {
            CLS_LOG(10, "ERROR: lsm_get_entries: failed to decode data size: %s", err.what());
            return -EINVAL;
        }
        size_to_process -= sizeof(uint64_t);

        if (data_size > size_to_process) {
            CLS_LOG(10, "INFO: lsm_get_entries: not enough data to read, breaking the loop...");
            break;
        }

        cls_lsm_entry entry;
        try {
            decode(entry, it);
        } catch (const ceph::buffer::error& err) {
            CLS_LOG(10, "ERROR: lsm_retrieve_data: failed to decode entry %s", err.what());
            return -EINVAL;
        }
        size_to_process -= data_size;

        // check if this is the entry wanted
        entries.emplace_back(entry);
    } while (size_to_process > 0);

    return 0;
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

    int ret = cls_cxx_write(hctx, 0, bl.length(), &bl);
    if (ret < 0) {
        CLS_LOG(5, "ERROR: lsm_write_node: failed to write lsm node");
        return ret;
    }

    return node_head.size;
}

/**
 * Write entries into the object
 */
int lsm_write_entries(cls_method_context_t hctx, std::vector<cls_lsm_entry>& entries)
{
    cls_lsm_node_head head;
    auto ret = lsm_read_node_head(hctx, head);
    if (ret < 0) {
        CLS_LOG(1, "ERROR: in lsm_write_data: failed reading node head");
        return ret;
    }

    for (auto entry : entries) {
        bufferlist bl;
        uint16_t entry_start = LSM_ENTRY_START;
        encode(entry_start, bl);

        bufferlist bl_data;
        encode(entry, bl_data);
        uint64_t encoded_len = bl_data.length();
        encode(encoded_len, bl);

        bl.claim_append(bl_data);

        ret = cls_cxx_write(hctx, head.data_end_offset, bl.length(), &bl);
        if (ret < 0) {
            CLS_LOG(1, "ERROR: lsm_write_data: failed to write the entry");
            return ret;
        }

        // update the node_head now that all data is written
        head.key_map[entry.key] = std::pair<uint64_t, uint64_t>(head.data_end_offset, head.data_end_offset+bl.length());
        head.data_end_offset += bl.length();
    }

    // write the node after all the entries are written
    ret = lsm_write_node_head(hctx, head);
    if (ret < 0) {
        CLS_LOG(1, "ERROR: lsm_write_data - failed updating node head");
        return ret;
    }

    return ret;
}

/**
 * Find the target object ids to scatter data into
 */
void lsm_get_scatter_targets(cls_lsm_node_head& head,
                            std::vector<std::vector<cls_lsm_entry>>& split_entries,
                            std::map<std::string, bufferlist>& tgt_child_objects)
{
    int level = get_level_from_object_id(head.object_id) + 1;
    std::string tree_name = get_tree_name_from_object_id(head.object_id);
    uint64_t columngroups = split_entries.size();

    std::map<std::string, std::vector<cls_lsm_entry>> target_entries_splits;
    uint64_t increment = (head.key_range.high_bound - head.key_range.low_bound) / head.key_range.splits;

    for (auto entries : split_entries) {

        for (auto entry : entries) {
            int key_group = (entry.key - head.key_range.low_bound) / increment;

            for (uint64_t colgrp = 0; colgrp < columngroups; colgrp++) {
                string obj_id = tree_name+"/level-"+to_string(level)+"/keyrange-"+to_string(key_group)+"/columngroup-"+to_string(colgrp);
                target_entries_splits[obj_id].push_back(entry);
            }

        }
    }

    for (auto target_entry : target_entries_splits) {

        bufferlist bl;
        encode(target_entry.second, bl);

        tgt_child_objects[target_entry.first] = bl;
    }
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

/**
 * sort batches during sort merge
 */
void sort_batches(std::map<int, std::vector<cls_lsm_entry> > batches, std::vector<cls_lsm_entry> new_batch, std::vector<cls_lsm_entry> sorted_batch)
{
    std::vector<cls_lsm_entry> start = batches[0];
    std::vector<cls_lsm_entry> result;

    for (uint64_t i = 1; i < batches.size(); i++) {
        sort_two(start, batches[i], result);
        start = result;
        result.clear();
    }

    sort_two(start, new_batch, sorted_batch);
}

/**
 * sort two batches at a time
 */
void sort_two(std::vector<cls_lsm_entry> start, std::vector<cls_lsm_entry> step, std::vector<cls_lsm_entry> result)
{
    uint64_t i = 0, j = 0;
    while (i < start.size() && j < step.size()) {
        if (start[i].key <= step[j].key) {
            result.push_back(start[i]);
            i++;
        } else {
            result.push_back(step[j]);
            j++;
        }
    }

    while (i < start.size()) {
        result.push_back(start[i]);
        i++;
    }

    while (j < step.size()) {
        result.push_back(step[j]);
        j++;
    }
}
