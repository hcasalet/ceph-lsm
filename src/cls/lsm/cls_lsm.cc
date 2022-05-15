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
#include "cls/lsm/cls_lsm_util.h"

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

    // initialize only the root node
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
    // get the write parameter
    auto iter = in->cbegin();
    cls_lsm_entry entry;
    try {
        decode(entry, iter);
    } catch (ceph::buffer::error& err) {
        CLS_LOG(1, "ERROR: cls_lsm_write_node: failed to decode input data");
        return -EINVAL;
    }

    // write or write with compaction
    std::vector<cls_lsm_entry> entries;
    entries.push_back(entry);
    auto ret = lsm_write_entries(hctx, entries);
    if (ret < 0) {
        CLS_LOG(1, "ERROR: cls_lsm_write_node: failed writing data");
        return ret;
    }

    return ret;
}

/**
 *  read data from node
 */
static int cls_lsm_read_node(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{   
    // getting the read parameter
    auto iter = in->cbegin();
    uint64_t key;
    try {
        decode(key, iter);
    } catch (ceph::buffer::error& err) {
        CLS_LOG(1, "ERROR: cls_lsm_read_node: failed to decode input data \n");
        return -EINVAL;
    }

    // get the entries from the object
    cls_lsm_entry entry;
    auto ret = lsm_read_data(hctx, key, entry);

    encode(entry, *out); 
    return ret;
}

/**
 * read data from an internal node
 */
static int cls_lsm_read_from_internal_nodes(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    /*auto it = in->cbegin();
    std::map<std::string, std::vector<uint64_t>> child_read_input;
    try {
        decode(child_read_input, it);
    } catch (const ceph::buffer::error& err) {
        CLS_LOG(1, "ERROR: lsm_read_from_internal_nodes: failed to decode getting input src objects: %s", err.what());
        return -EINVAL;
    }*/

    cls_lsm_node_head root;
    auto ret = lsm_read_node_head(hctx, root);
    if (ret < 0) {
        return ret;
    }

    ret = cls_cxx_read(hctx, root.data_start_offset, (root.data_end_offset - root.data_start_offset), out);
    CLS_LOG(1, "Holly debug reading from internal nodes: %s", out->c_str());
    if (ret < 0) {
        CLS_LOG(1, "ERROR: cls_lsm_read_from_internal_nodes: failed reading the chunk");
        return ret;
    }

    return 0;
}

/**
 * Function to prepare a compaction
 */
int lsm_prepare_compaction(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    auto itr = in->cbegin();
    std::vector<std::vector<std::string>> col_grps;
    try {
        decode(col_grps, itr);
    } catch (const ceph::buffer::error& err) {
        CLS_LOG(1, "ERROR: lsm_prepare_compaction: failed to prepare compaction: %s", err.what());
        return -EINVAL;
    }

    cls_lsm_node_head head;
    auto ret = lsm_read_node_head(hctx, head);
    if (ret < 0) {
        return ret;
    }

    bufferlist bl_chunk;
    ret = cls_cxx_read(hctx, head.data_start_offset, (head.data_end_offset - head.data_start_offset), &bl_chunk);
    if (ret < 0) {
        CLS_LOG(1, "ERROR: lsm_prepare_compaction: failed to read data from root");
        return ret;
    }
    std::vector<cls_lsm_entry> entries;
    ret = lsm_get_entries(&bl_chunk, entries);
    if (ret < 0) {
        CLS_LOG(1, "ERROR: lsm_prepare_compaction: failed to get entries from root");
        return ret;
    }

    // split the entries by columns
    std::vector<std::vector<cls_lsm_entry>> split_entries;
    split_column_groups_for_entries(entries, col_grps, split_entries);

    // get the target objects
    std::map<std::string, bufferlist> tgt_objects; 
    lsm_get_scatter_targets(head, split_entries, tgt_objects);

    encode(tgt_objects, *out);
    encode(head.pool, *out);

    return 0;
}

/**
 * Function to compact
 */
int lsm_compact(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    int r = cls_cxx_scatter_wait_for_completions(hctx);
    if (r == -EAGAIN) {
        auto in_iter = in->cbegin();
        std::map<std::string, bufferlist> tgt_objects;
        try {
            decode(tgt_objects, in_iter);
        } catch (const ceph::buffer::error& err) {
            CLS_LOG(1, "ERROR: lsm_compact: failed to decode tgt objects: %s", err.what());
            return -EINVAL;
        }

        std::string pool;
        try {
            decode(pool, in_iter);
        } catch (const ceph::buffer::error& err) {
            CLS_LOG(1, "ERROR: lsm_compact: failed to decode pool: %s", err.what());
            return -EINVAL;
        }

        r = cls_cxx_scatter(hctx, tgt_objects, pool, LSM_CLASS, LSM_COMPACT_ENTRIES_TO_TARGETS, *in);
    } else {
        if (r != 0) {
            CLS_ERR("%s: remote write failed. error=%d", __PRETTY_FUNCTION__, r);
        }
    }
    return r;
}

/**
 * Write entries into the object node
 */
int lsm_compact_entries_to_targets(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    auto itt = in->cbegin();
    std::vector<cls_lsm_entry> new_entries;
    try {
        decode(new_entries, itt);
    } catch (const ceph::buffer::error& err) {
        CLS_LOG(1, "ERROR: lsm_compact_entries_to_targets: failed to decode entries: %s", err.what());
        return -EINVAL;
    }

    auto r = lsm_write_entries(hctx, new_entries);

    return r;
}

/**
 * Function to update the compacted node
 */
int lsm_update_post_compaction(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    cls_lsm_node_head head;
    auto ret = lsm_read_node_head(hctx, head);
    if (ret < 0) {
        CLS_LOG(1, "ERROR: in lsm_update_post_compaction: failed reading node head");
        return ret;
    }

    head.size = 0;
    head.data_end_offset = head.data_start_offset;
    head.key_map.clear();

    ret = lsm_write_node_head(hctx, head);
    if (ret < 0) {
        CLS_LOG(1, "ERROR: in lsm_update_post_compaction: failed writing node head back");
        return ret;
    }
 
    return 0;
}

/**
 * Function to get all child object ids to prepare for gathering
 */
/*int lsm_prepare_gathering(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    auto in_iter = in->cbegin();
    cls_lsm_get_entries_op op;
    try {
        decode(op, in_iter);
    } catch (const ceph::buffer::error& err) {
        CLS_LOG(1, "ERROR: lsm_prepare_gathering: failed to decode input: %s", err.what());
        return -EINVAL;
    }

    cls_lsm_node_head root;
    auto ret = lsm_read_node_head(hctx, root);
    if (ret < 0) {
        return ret;
    }

    std::set<std::string> child_objs;
    lsm_get_child_object_ids(root, op.keys, op.columns, child_objs);

    encode(child_objs, *out);
    encode(root.pool, *out);

    return 0;
}*/

/**
 * Function to gather
 */
int lsm_gather(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    std::map<std::string, bufferlist> src_obj_buffs;
    int r = cls_cxx_get_gathered_data(hctx, &src_obj_buffs);
    if (src_obj_buffs.empty()) {
        auto in_iter = in->cbegin();
        std::vector<std::string> child_objs_v;
        try {
            decode(child_objs_v, in_iter);
        } catch (const ceph::buffer::error& err) {
            CLS_LOG(1, "ERROR: lsm_gather: failed to decode input: %s", err.what());
            return -EINVAL;
        }
        std::set<std::string> child_objs(child_objs_v.begin(), child_objs_v.end());

        std::string pool_name;
        try {
            decode(pool_name, in_iter);
        } catch (const ceph::buffer::error& err) {
            CLS_LOG(1, "ERROR: lsm_gather: failed to decode pool: %s", err.what());
            return -EINVAL;
        }

        r = cls_cxx_gather(hctx, child_objs, pool_name, LSM_CLASS, LSM_READ_NODE, *in);
    } else {
        cls_lsm_entry entry_result;
        bool entry_result_initialized = false;
        for (std::map<std::string, bufferlist>::iterator it = src_obj_buffs.begin(); it != src_obj_buffs.end(); it++){
            bufferlist bl= it->second;
            auto itr = bl.cbegin();
            cls_lsm_entry entry;
            try {
                decode(entry, itr);
            } catch (const ceph::buffer::error& err) {
                CLS_LOG(1, "ERROR: lsm_gather: failed to decode pool: %s", err.what());
            }

            if (!entry_result_initialized) {
                entry_result = entry;
                entry_result_initialized = true;
            } else {
                entry_result.value.insert(entry.value.begin(), entry.value.end());
            }
        }
        encode(entry_result, *out);
    }

    return r;
}

CLS_INIT(lsm)
{
    CLS_LOG(1, "Loaded lsm class!");

    cls_handle_t h_class;
    cls_method_handle_t h_lsm_init;
    cls_method_handle_t h_lsm_write_node;
    cls_method_handle_t h_lsm_read_node;
    cls_method_handle_t h_lsm_read_from_internal_nodes;
    cls_method_handle_t h_lsm_compact_entries_to_targets;
    cls_method_handle_t h_lsm_prepare_compaction;
    cls_method_handle_t h_lsm_compact;
    cls_method_handle_t h_lsm_update_post_compaction;
    //cls_method_handle_t h_lsm_prepare_gathering;
    cls_method_handle_t h_lsm_gather;

    cls_register(LSM_CLASS, &h_class);

    cls_register_cxx_method(h_class, LSM_INIT, CLS_METHOD_RD | CLS_METHOD_WR, cls_lsm_init, &h_lsm_init);
    cls_register_cxx_method(h_class, LSM_WRITE_NODE, CLS_METHOD_RD | CLS_METHOD_WR, cls_lsm_write_node, &h_lsm_write_node);
    cls_register_cxx_method(h_class, LSM_READ_NODE, CLS_METHOD_RD | CLS_METHOD_WR, cls_lsm_read_node, &h_lsm_read_node);
    cls_register_cxx_method(h_class, LSM_READ_FROM_INTERNAL_NODES, CLS_METHOD_RD | CLS_METHOD_WR, cls_lsm_read_from_internal_nodes, &h_lsm_read_from_internal_nodes);
    cls_register_cxx_method(h_class, LSM_COMPACT_ENTRIES_TO_TARGETS, CLS_METHOD_RD | CLS_METHOD_WR, lsm_compact_entries_to_targets, &h_lsm_compact_entries_to_targets);
    cls_register_cxx_method(h_class, LSM_PREPARE_COMPACTION, CLS_METHOD_RD | CLS_METHOD_WR, lsm_prepare_compaction, &h_lsm_prepare_compaction);
    cls_register_cxx_method(h_class, LSM_COMPACT, CLS_METHOD_RD | CLS_METHOD_WR, lsm_compact, &h_lsm_compact);
    cls_register_cxx_method(h_class, LSM_UPDATE_POST_COMPACTION, CLS_METHOD_RD | CLS_METHOD_WR, lsm_update_post_compaction, &h_lsm_update_post_compaction);
    //cls_register_cxx_method(h_class, LSM_PREPARE_GATHERING, CLS_METHOD_RD | CLS_METHOD_WR, lsm_prepare_gathering, &h_lsm_prepare_gathering);
    cls_register_cxx_method(h_class, LSM_GATHER, CLS_METHOD_RD | CLS_METHOD_WR, lsm_gather, &h_lsm_gather);

    return; 
}