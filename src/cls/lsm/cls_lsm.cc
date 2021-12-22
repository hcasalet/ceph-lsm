//
// Created by Holly Casaletto on 7/29/21.
//

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
    std::map<std::string, std::vector<uint64_t>> src_objs_map;
    auto ret = lsm_read_data(hctx, op, op_ret, src_objs_map);
    if (ret < 0) {
        return ret;
    }
    encode(op_ret, *out);
    encode(src_objs_map, *out);
       
    return 0;
}

/**
 * read data from an internal node
 */
static int cls_lsm_read_from_internal_nodes(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    auto it = in->cbegin();
    std::map<std::string, std::vector<uint64>> src_objs_map;
    try {
        decode(src_objs_map, it);
    } catch (const ceph::buffer::error& err) {
        CLS_LOG(1, "ERROR: lsm_read_from_internal_nodes: failed to decode getting input src objects: %s", err.what());
        return -EINVAL;
    }

    std::set<std::string> src_objs;
    for (std::map<std::string, std::vector<uint64_t>>::iterator it = src_objs_map.begin(); it != src_objs_map.end(); it++) {
        src_objs.insert(it->first);
    }

    std::vector<std::string> cols;
    try {
        decode(cols, it);
    } catch (const ceph::buffer::error& err) {
        CLS_LOG(1, "ERROR: lsm_read_from_internal_nodes: failed to decode getting input get_entries_op: %s", err.what());
        return -EINVAL;
    }

    cls_lsm_tree_config tree;
    auto ret = lsm_read_tree_config(hctx, tree);

    std::map<std::string, bufferlist> src_obj_buffs;
    int r = cls_cxx_get_gathered_data(hctx, &src_obj_buffs);
    if (src_obj_buffs.empty()) {
        r = cls_cxx_gather(hctx, src_objs, tree.pool, LSM_CLASS, LSM_REMOTE_READ_NODE, *in);
    } else {
        cls_lsm_get_entries_ret op_ret_all;
        std::map<std::string, std::vector<uint64_t>> child_src_objs_map;
        for (std::map<std::string, bufferlist>::iterator it = src_obj_buffs.begin(); it != src_obj_buffs.end(); it++) {
            bufferlist bl= it->second;

            auto itr = bl.cbegin();

            cls_lsm_node_head node_head;
            try {
                decode(node_head, itr);
            } catch (const ceph::buffer::error& err) {
                CLS_LOG(1, "ERROR: lsm_read_from_internal_nodes: failed to decode gathered data: %s", err.what());
                return -EINVAL;
            }

            std::vector<uint64_t> found_keys;
            auto ret = lsm_check_if_key_exists(node_head.bloomfilter_store, src_objs_map[it->first], found_keys);

            if (found_keys.size() > 0) {
                cls_lsm_get_entries_ret op_ret;
                ret = lsm_get_entries(itr, found_keys, &op_ret);
                op_ret_all.entries.insert(op_ret_all.entries.end(), op_ret.entries.begin(), op.ret.entries.end());
            }

            for (auto found_key : found_keys) {
                src_objs_map[it->first].erase(std::remove(src_objs_map[it->first].begin(), src_objs_map[it->first].end(), found_key), src_objs_map[it->first].end());
            }

            if (src_objs_map[it->first].size() > 0) {
                ret = lsm_get_child_object_ids(node_head, src_objs_map[it->first], cols, child_src_objs_map);
                if (ret < 0) {
                    CLS_LOG(1, "ERROR: cls_lsm_read_from_internal_nodes: failed to get child object ids");
                    return ret;
                }
            }
        }
        encode(op_ret_all, *out);
        encode(child_src_objs_map, *out);
    }

    return 0;
}

/**
 * Remote read node
 */
int lsm_remote_read_node(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    ret = cls_cxx_read(hctx, 0, 0, out);
    if (ret < 0) {
        CLS_LOG(1, "ERROR: lsm_remote_read_node: error reading data");
        return ret;
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

    std::vector<std::set<std::string>> column_splits;
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
        return -ENODATA;
    }

    CLS_LOG(1, "ocean : %lu", new_entries.size());
    // check if the object exists and if has data in it
    uint64_t read_size = CHUNK_SIZE, start_offset = 0;
    bufferlist bl_head;
    auto ret = cls_cxx_read(hctx, start_offset, read_size, &bl_head);
    if (ret  < 0) {
        CLS_LOG(1, "ERROR: lsm_write_to_internal_node: failed checking if node exists");
        return ret;
    } else if (ret > 0) {
        CLS_LOG(1, "water street!!!");
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
        new_node_head.column_group_splits = column_splits;
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
    cls_method_handle_t h_lsm_write_to_internal_nodes;
    cls_method_handle_t h_lsm_remote_read_node;

    cls_register(LSM_CLASS, &h_class);

    cls_register_cxx_method(h_class, LSM_INIT, CLS_METHOD_RD | CLS_METHOD_WR, cls_lsm_init, &h_lsm_init);
    cls_register_cxx_method(h_class, LSM_WRITE_NODE, CLS_METHOD_RD | CLS_METHOD_WR, cls_lsm_write_node, &h_lsm_write_node);
    cls_register_cxx_method(h_class, LSM_READ_NODE, CLS_METHOD_RD | CLS_METHOD_WR, cls_lsm_read_node, &h_lsm_read_node);
    cls_register_cxx_method(h_class, LSM_READ_FROM_INTERNAL_NODES, CLS_METHOD_RD | CLS_METHOD_WR, cls_lsm_read_from_internal_nodes, &h_lsm_read_from_internal_nodes);
    cls_register_cxx_method(h_class, LSM_WRITE_TO_INTERNAL_NODES, CLS_METHOD_RD | CLS_METHOD_WR, lsm_write_to_internal_nodes, &h_lsm_write_to_internal_nodes);
    cls_register_cxx_method(h_class, LSM_REMOTE_READ_NODE, CLS_METHOD_RD | CLS_METHOD_WR, lsm_remote_read_node, &h_lsm_remote_read_node);

    return; 
}