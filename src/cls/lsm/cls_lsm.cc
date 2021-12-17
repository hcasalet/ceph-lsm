//
// Created by Holly Casaletto on 7/29/21.
//

// TO-DO: 
// 1. cls_lsm_init_tree
// 2. bloom filter

#include "include/types.h"
#include <errno.h>

#include "objclass/objclass.h"
#include "cls/lsm/cls_lsm_types.h"
#include "cls/lsm/cls_lsm_const.h"
#include "cls/lsm/cls_lsm_ops.h"
#include "cls/lsm/cls_lsm_src.h"
#include "cls/lsm/cls_lsm_bloomfilter.h"

using ceph::bufferlist;
using ceph::decode;
using ceph::encode;

CLS_VER(1,0)
CLS_NAME(lsm)

/**
 * initialize an lsm tree node
 */
static int cls_lsm_init(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    auto in_iter = in->cbegin();
    cls_lsm_init_op op;
    try {
        decode(op, in_iter);
    } catch (ceph::buffer::error& err) {
        CLS_LOG(1, "ERROR: cls_lsm_init_op: failed to decdoe entry\n");
        return -EINVAL;
    }

    // initialize only the level-0 node
    auto ret = lsm_init(hctx, op);
    if (ret < 0) {
        return ret;
    }

    return 0;
}

/**
 * write data to node
 */ 
static int cls_lsm_write_node(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    // first get the input
    auto iter = in->cbegin();
    cls_lsm_append_entries_op op;
    try {
        decode(op, iter);
    } catch (ceph::buffer::error& err) {
        CLS_LOG(1, "ERROR: cls_lsm_write_node: failed to decode input data \n");
        return -EINVAL;
    }

    // Write the data
    auto ret = lsm_write_data(hctx, op);
    if (ret < 0) {
        return ret;
    }

    return 0;
}

/**
 *  read data from node
 */
static int cls_lsm_read_node(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    auto iter = in->cbegin();
    cls_lsm_get_entries_op op;
    try {
        decode(op, iter);
    } catch (ceph::buffer::error& err) {
        CLS_LOG(1, "ERROR: cls_lsm_read_node: failed to decode input data \n");
        return -EINVAL;
    }

    cls_lsm_get_entries_ret op_ret;
    auto ret = lsm_read_data(hctx, op, op_ret);
    if (ret < 0) {
        return ret;
    }

    encode(op_ret, *out);
    return 0;
}

/**
 * read data from an internal node
 */
static int cls_lsm_read_from_internal_node(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    auto it = in->cbegin();
    cls_lsm_get_entries_op op;
    try {
        decode(op, it);
    } catch (const ceph::buffer::error& err) {
        CLS_LOG(1, "ERROR: lsm_read_from_internal_nodes: failed to decode getting entries input: %s", err.what());
        return -EINVAL;
    }

    cls_lsm_node_head node_head;
    auto ret = lsm_read_node_head(hctx, node_head);
    if (ret < 0) {
        return ret;
    }

    std::vector<uint64_t> found_keys;
    ret = lsm_check_if_key_exists(node_head.bloomfilter_store, op.keys, found_keys);
    cls_lsm_get_entries_ret op_ret;
    if (found_keys.size() >= op.keys.size()) {
        bufferlist bl_chunk;
        ret = cls_cxx_read(hctx, node_head.entry_start_offset, (node_head.entry_end_offset - node_head.entry_start_offset), &bl_chunk);

        ret = lsm_get_entries(hctx, &bl_chunk, found_keys, op_ret);
        encode(op_ret, *out);
    } else {
        for (auto key : found_keys) {
            op.keys.erase(std::remove(op.keys.begin(), op.keys.end(), key), op.keys.end());
        }

        ret = lsm_read_from_below_level_0(hctx, node_head.key_range, node_head.column_group_splits, node_head.my_object_id,
                                        node_head.pool, op, op_ret);
    }

    return 0;
}

/**
 * Function to write the internal nodes
 */
int lsm_write_to_internal_nodes(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    auto itt = in->cbegin();
    std::string object_name;
    try {
        decode(object_name, itt);
    } catch (const ceph::buffer::error& err) {
        CLS_LOG(1, "ERROR: lsm_write_to_internal_node: failed to decode object name: %s", err.what());
        return -EINVAL;
    }

    uint64_t my_level;
    try {
        decode(my_level, itt);
    } catch (const ceph::buffer::error& err) {
        CLS_LOG(1, "ERROR: lsm_write_to_internal_node: failed to decode the level: %s", err.what());
        return -EINVAL;
    }

    cls_lsm_key_range key_range;
    try {
        decode(key_range, itt);
    } catch (const ceph::buffer::error& err) {
        CLS_LOG(1, "ERROR: lsm_write_to_internal_node: failed to decode key range: %s", err.what());
        return -EINVAL;
    }

    uint64_t node_capacity;
    try {
        decode(node_capacity, itt);
    } catch (const ceph::buffer::error& err) {
        CLS_LOG(1, "ERROR: lsm_write_to_internal_node: failed to decode node capacity: %s", err.what());
        return -EINVAL;
    }

    std::set<std::string> column_splits;
    try {
        decode(column_splits, itt);
    } catch (const ceph::buffer::error& err) {
        CLS_LOG(1, "ERROR: lsm_write_to_internal_node: failed to decode column splits: %s", err.what());
        return -EINVAL;
    }
    if (column_splits.size() == 0) {
        CLS_LOG(1, "ERROR: lsm_write_to_internal_node: failed reading column_splits for the node");
    }

    std::vector<cls_lsm_entry> new_entries;
    try {
        decode(new_entries, itt);
    } catch (const ceph::buffer::error& err) {
        CLS_LOG(1, "ERROR: lsm_write_to_internal_node: failed to decode entries: %s", err.what());
        return -EINVAL;
    }
    if (new_entries.size() == 0) {
        CLS_LOG(1, "ERROR: lsm_write_to_internal_node: failed getting entries from the input");
    }

    // check if the object exists and if has data in it
    uint64_t read_size = CHUNK_SIZE, start_offset = 0;
    bufferlist bl_head;
    auto ret = cls_cxx_read(hctx, start_offset, read_size, &bl_head);
    if (ret  < 0) {
        CLS_LOG(1, "ERROR: lsm_write_to_internal_node: failed checking if node exists");
        return ret;
    } else if (ret > 0) {
        auto it = bl_head.cbegin();

        // check node head start
        uint16_t node_start;
        try {
            decode(node_start, it);
        } catch (const ceph::buffer::error& err) {
            CLS_LOG(1, "ERROR: lsm_write_to_internal_node: failed to decode node head start");
            return -EINVAL;
        }
        if (node_start != LSM_NODE_START) {
            CLS_LOG(1, "ERROR: lsm_write_to_internal_node: invalid node start");
            return -EINVAL;
        }

        // check node length
        uint64_t node_length;
        try {
            decode(node_length, it);
        } catch (const ceph::buffer::error& err) {
            CLS_LOG(0, "ERROR: lsm_write_to_internal_node: failed to decode node length");
            return -EINVAL;
        }

        // if read-in length is not covering the whole tree, need to read more
        if (read_size - sizeof(uint16_t) - sizeof(uint64_t) < node_length) {
            CLS_LOG(1, "in lsm_write_to_internal_node, and need to read again!");
            uint64_t continue_to_read = node_length - read_size + sizeof(uint16_t) + sizeof(uint64_t);
            start_offset += read_size;
            bufferlist bl_head_2;
            ret = cls_cxx_read(hctx, start_offset, continue_to_read, &bl_head_2);

            if (ret < 0) {
                CLS_LOG(1, "ERROR: lsm_write_to_internal_node: failed to read the rest of the node");
                return ret;
            }
            bl_head.claim_append(bl_head_2);
        }

        // get tree config
        cls_lsm_node_head node_head;
        try {
            decode(node_head, it);
        } catch (const ceph::buffer::error& err) {
            CLS_LOG(0, "ERROR: lsm_read_node: failed to decode node: %s", err.what());
            return -EINVAL;
        }

        if (node_head.size + new_entries.size() < node_head.max_capacity) {
            ret = lsm_write_one_node(hctx, node_head, new_entries);
            if (ret < 0) {
                return ret;
            }
        } else {
            // read in the data that the object already has
            bufferlist bl_chunk;
            ret = cls_cxx_read(hctx, node_head.entry_start_offset, (node_head.entry_end_offset - node_head.entry_start_offset), &bl_chunk);

            std::vector<uint64_t> found_keys;
            cls_lsm_get_entries_ret op_ret;
            ret = lsm_get_entries(hctx, &bl_chunk, found_keys, op_ret);

            new_entries.insert(new_entries.end(), op_ret.entries.begin(), op_ret.entries.end());

            ret = lsm_compact_node(hctx, new_entries, node_head);
            if (ret < 0) {
                return ret;
            }
        }
    } else {
        cls_lsm_node_head new_node_head;
        new_node_head.my_object_id = object_name;
        new_node_head.my_level = my_level;
        new_node_head.key_range = key_range;
        new_node_head.max_capacity = node_capacity;
        new_node_head.size = 0;
        new_node_head.column_group_splits = lsm_make_column_group_splits_for_children(column_splits, LSM_COLUMN_SPLIT_FACTOR);
        new_node_head.bloomfilter_store = std::vector<bool>(BLOOM_FILTER_STORE_SIZE_64K, false);

        bufferlist bl;
        encode(new_node_head, bl);

        new_node_head.entry_start_offset = bl.length() + sizeof(uint64_t) * 3 + sizeof(uint16_t) + LSM_DATA_START_PADDING;
        new_node_head.entry_end_offset = new_node_head.entry_start_offset;

        ret = lsm_write_one_node(hctx, new_node_head, new_entries);
        if (ret < 0) {
            return ret;
        }
    }

    return 0;
}

CLS_INIT(lsm)
{
    CLS_LOG(1, "Loaded lsm class!");

    cls_handle_t h_class;
    cls_method_handle_t h_lsm_init;
    cls_method_handle_t h_lsm_write_node;
    cls_method_handle_t h_lsm_read_node;
    cls_method_handle_t h_lsm_read_from_internal_nodes;

    cls_register(LSM_CLASS, &h_class);

    cls_register_cxx_method(h_class, LSM_INIT, CLS_METHOD_RD | CLS_METHOD_WR, cls_lsm_init, &h_lsm_init);
    cls_register_cxx_method(h_class, LSM_WRITE_NODE, CLS_METHOD_RD | CLS_METHOD_WR, cls_lsm_write_node, &h_lsm_write_node);
    cls_register_cxx_method(h_class, LSM_READ_NODE, CLS_METHOD_RD | CLS_METHOD_WR, cls_lsm_read_node, &h_lsm_read_node);
    cls_register_cxx_method(h_class, LSM_READ_FROM_INTERNAL_NODES, CLS_METHOD_RD, cls_lsm_read_from_internal_node, &h_lsm_read_from_internal_nodes);
    
    return; 
}