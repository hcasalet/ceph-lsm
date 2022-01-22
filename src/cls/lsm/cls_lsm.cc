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
    cls_lsm_append_entries_op op;
    try {
        decode(op, iter);
    } catch (ceph::buffer::error& err) {
        CLS_LOG(1, "ERROR: cls_lsm_write_node: failed to decode input data");
        return -EINVAL;
    }

    // read from root to prepare to write
    cls_lsm_node_head root;
    auto ret = lsm_read_node_head(hctx, root);
    if (ret < 0) {
        CLS_LOG(1, "ERROR: cls_lsm_write_node: failed reading tree config");
        return ret;
    }
    
    // write or write with compaction
    CLS_LOG(1, "DEBUG: entering write, root size %lu, root capacity %lu, entry %lu", root.size, root.capacity, op.entries[0].key);
    ret = lsm_write_one_node(hctx, root, op.entries);
    if (ret < 0) {
        CLS_LOG(1, "ERROR: cls_lsm_write_node: failed writing data");
        return ret;
    }

    return 0;
}

/**
 *  read data from node
 */
static int cls_lsm_read_node(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{   
    // getting the read parameter
    auto iter = in->cbegin();
    cls_lsm_get_entries_op op;
    try {
        decode(op, iter);
    } catch (ceph::buffer::error& err) {
        CLS_LOG(1, "ERROR: cls_lsm_read_node: failed to decode input data \n");
        return -EINVAL;
    }

    // read from root to check if data exists in root node
    cls_lsm_node_head root;
    auto ret = lsm_read_node_head(hctx, root);
    if (ret < 0) {
        CLS_LOG(1, "ERROR: cls_lsm_read_node: failed reading tree config");
        return ret;
    }

    // check if keys exist in the entire system at all
    std::vector<uint64_t> keys_in_system;
    CLS_LOG(1, "Holly debug: key to look for is %lu", op.keys[0]);
    lsm_check_if_key_exists(root.bloomfilter_store_ever, op.keys, keys_in_system);
    if (keys_in_system.size() == 0) {
        CLS_LOG(1, "INFO: keys to read do not exist in the system");
        return 0;
    }

    // check if the root node has the keys
    std::vector<uint64_t> keys_in_root;
    lsm_check_if_key_exists(root.bloomfilter_store, op.keys, keys_in_root);

    // get the entries from the object
    op.keys.clear();
    cls_lsm_get_entries_ret op_ret;
    if (keys_in_root.size() == keys_in_system.size()) {
        CLS_LOG(1, "DEBUG: reading only in root");
        op.keys.insert(op.keys.end(), keys_in_root.begin(), keys_in_root.end());
        ret = lsm_read_data(hctx, root, op, op_ret);
    } else {
        op.keys.insert(op.keys.end(), keys_in_system.begin(), keys_in_system.end());
        CLS_LOG(1, "DEBUG: reading from internal nodes: key size %lu", op.keys.size());
        ret = lsm_read_with_gathering(hctx, root, op, op_ret);
    }

    encode(op_ret, *out); 
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

    CLS_LOG(1, "Holly debug: end: %lu, start %lu", root.entry_end_offset, root.entry_start_offset);
    ret = cls_cxx_read(hctx, root.entry_start_offset, (root.entry_end_offset - root.entry_start_offset), out);
    CLS_LOG(1, "Holly debug reading from internal nodes: %s", out->c_str());
    if (ret < 0) {
        CLS_LOG(1, "ERROR: cls_lsm_read_from_internal_nodes: failed reading the chunk");
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
    bool is_root;
    try {
        decode(is_root, itt);
    } catch (const ceph::buffer::error& err) {
        CLS_LOG(1, "ERROR: lsm_write_to_internal_node: failed to decode is_root: %s", err.what());
        return -EINVAL;
    }

    cls_lsm_node_head new_head;
    try {
        decode(new_head, itt);
    } catch (const ceph::buffer::error& err) {
        CLS_LOG(1, "ERROR: lsm_write_to_internal_node: failed to decode node head: %s", err.what());
        return -EINVAL;
    }

    std::vector<cls_lsm_entry> new_entries;
    try {
        decode(new_entries, itt);
    } catch (const ceph::buffer::error& err) {
        CLS_LOG(1, "ERROR: lsm_write_to_internal_node: failed to decode entries: %s", err.what());
        return -EINVAL;
    }

    // Casae 1: if this is the compacting root node, just write the new root node
    if (is_root) {
        lsm_write_one_node(hctx, new_head, new_entries);
        return 0;
    }

    // Case 2, 3, 4, 5 are for the children nodes:
    // Case 2, node does not exist; 
    // Case 3, node exists and it is the leaf node;
    // Case 4, node exists and it is not a leaf node but there is space; 
    // Case 5, node exists but is full so needs to invoke compaction (TODO!!!!)
    cls_lsm_node_head old_head;
    auto ret = lsm_read_node_head(hctx, old_head);

    if (ret < 0) {
        CLS_LOG(1, "Holly debug just write, head name %s", new_head.my_object_id.c_str());
        ret = lsm_write_one_node(hctx, new_head, new_entries);
    } else {
        CLS_LOG(1, "Holly debug write to old %s", old_head.my_object_id.c_str());
        ret = lsm_write_one_node(hctx, old_head, new_entries);
    }

    return ret;
}

/**
 * Function to create an object
 */
int lsm_create_object(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    auto in_iter = in->cbegin();

    cls_lsm_node_head head;
    try {
        decode(head, in_iter);
    } catch (const ceph::buffer::error& err) {
        CLS_LOG(1, "ERROR: lsm_create_object: failed to decode head: %s", err.what());
        return -EINVAL;
    }

    std::vector<cls_lsm_entry> entries;
    try {
        decode(entries, in_iter);
    } catch (const ceph::buffer::error& err) {
        CLS_LOG(1, "ERROR: lsm_create_object: failed to decode entries: %s", err.what());
        return -EINVAL;
    }

    std::vector<uint64_t> keys_in_system;
    try {
        decode(keys_in_system, in_iter);
    } catch (const ceph::buffer::error& err) {
        CLS_LOG(1, "ERROR: lsm_create_object: failed to decode other keys: %s", err.what());
    }
    for (auto key : keys_in_system) {
        CLS_LOG(1, "Holly debug: inserting key %lu", key);
        lsm_bloomfilter_insert(head.bloomfilter_store_ever, to_string(key));
    }

    auto ret = lsm_write_one_node(hctx, head, entries);
    if (ret < 0) {
        CLS_LOG(1, "ERROR: ");
        return ret; 
    }
    return 0;
}

/**
 * Function to prepare a compaction
 */
int lsm_prepare_compaction(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    cls_lsm_node_head root;
    auto ret = lsm_read_node_head(hctx, root);
    if (ret < 0) {
        return ret;
    }

    bufferlist bl_chunk;
    ret = cls_cxx_read(hctx, root.entry_start_offset, (root.entry_end_offset - root.entry_start_offset), &bl_chunk);
    if (ret < 0) {
        CLS_LOG(1, "ERROR: lsm_write_with_compaction: failed to read data from root");
        return ret;
    }
    std::vector<uint64_t> empty_keys;
    cls_lsm_get_entries_ret old_entries_ret;
    ret = lsm_get_entries(&bl_chunk, empty_keys, old_entries_ret);
    if (ret < 0) {
        CLS_LOG(1, "ERROR: lsm_write_with_compaction: failed to get entries from root");
        return ret;
    }

    // split the entries
    std::map<std::string, bufferlist> tgt_objects; 
    lsm_get_scatter_targets(root, old_entries_ret.entries, tgt_objects);
    CLS_LOG(1, "Holly debug: in prepare compaction, tgt_objs size %lu", tgt_objects.size());
    for (auto tgt_object : tgt_objects) {
        CLS_LOG(1, "Holly debug: tgt object %s", tgt_object.first.c_str());
    }

    encode(tgt_objects, *out);
    encode(root.pool, *out);

    return 0;
}

/**
 * Function to compact
 */
int lsm_compact(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    CLS_LOG(1, "Holly debug scatter");
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

        r = cls_cxx_scatter(hctx, tgt_objects, pool, LSM_CLASS, LSM_WRITE_TO_INTERNAL_NODES, *in);
    } else {
        if (r != 0) {
            CLS_ERR("%s: remote write failed. error=%d", __PRETTY_FUNCTION__, r);
        }
    }
    return r;
}

/**
 * Function to update the compacted node
 */
int lsm_update_post_compaction(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    cls_lsm_node_head root;
    auto ret = lsm_read_node_head(hctx, root);
    if (ret < 0) {
        CLS_LOG(1, "ERROR: in lsm_update_post_compaction: failed reading node head");
        return ret;
    }

    root.size = 0;
    root.entry_end_offset = root.entry_start_offset;
    root.bloomfilter_store = std::vector<bool>(BLOOM_FILTER_STORE_SIZE_256K, false);

    ret = lsm_write_node_head(hctx, root);
    if (ret < 0) {
        CLS_LOG(1, "ERROR: in lsm_update_post_compaction: failed writing node head back");
        return ret;
    }
 
    return 0;
}

/**
 * Function to get all child object ids to prepare for gathering
 */
int lsm_prepare_gathering(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
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
}

/**
 * Function to gather
 */
int lsm_gather(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    std::map<std::string, bufferlist> src_obj_buffs;
    int r = cls_cxx_get_gathered_data(hctx, &src_obj_buffs);
    if (src_obj_buffs.empty()) {
        auto in_iter = in->cbegin();
        std::set<std::string> child_objs;
        try {
            decode(child_objs, in_iter);
        } catch (const ceph::buffer::error& err) {
            CLS_LOG(1, "ERROR: lsm_gather: failed to decode input: %s", err.what());
            return -EINVAL;
        }

        std::string pool;
        try {
            decode(pool, in_iter);
        } catch (const ceph::buffer::error& err) {
            CLS_LOG(1, "ERROR: lsm_gather: failed to decode pool: %s", err.what());
            return -EINVAL;
        }

        CLS_LOG(1, "Holly debug gather, child_objs size %lu", child_objs.size());
        r = cls_cxx_gather(hctx, child_objs, pool, LSM_CLASS, LSM_READ_FROM_INTERNAL_NODES, *in);
    } else {
        std::vector<uint64_t> found_keys;
        cls_lsm_get_entries_ret op_ret;
        for (std::map<std::string, bufferlist>::iterator it = src_obj_buffs.begin(); it != src_obj_buffs.end(); it++){
            bufferlist bl= it->second;
            cls_lsm_get_entries_ret child_ret;
            r = lsm_get_entries(&bl, found_keys, child_ret);
            CLS_LOG(1, "Holly debug child_ret %lu", child_ret.entries[0].key);
            op_ret.entries.insert(op_ret.entries.end(), child_ret.entries.begin(), child_ret.entries.end());
        }
        CLS_LOG(1, "Holly debug final checking %lu, size %lu", op_ret.entries[0].key, op_ret.entries.size());
        encode(op_ret, *out);
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
    cls_method_handle_t h_lsm_write_to_internal_nodes;
    cls_method_handle_t h_lsm_create_object;
    cls_method_handle_t h_lsm_prepare_compaction;
    cls_method_handle_t h_lsm_compact;
    cls_method_handle_t h_lsm_update_post_compaction;
    cls_method_handle_t h_lsm_prepare_gathering;
    cls_method_handle_t h_lsm_gather;

    cls_register(LSM_CLASS, &h_class);

    cls_register_cxx_method(h_class, LSM_INIT, CLS_METHOD_RD | CLS_METHOD_WR, cls_lsm_init, &h_lsm_init);
    cls_register_cxx_method(h_class, LSM_WRITE_NODE, CLS_METHOD_RD | CLS_METHOD_WR, cls_lsm_write_node, &h_lsm_write_node);
    cls_register_cxx_method(h_class, LSM_READ_NODE, CLS_METHOD_RD | CLS_METHOD_WR, cls_lsm_read_node, &h_lsm_read_node);
    cls_register_cxx_method(h_class, LSM_READ_FROM_INTERNAL_NODES, CLS_METHOD_RD | CLS_METHOD_WR, cls_lsm_read_from_internal_nodes, &h_lsm_read_from_internal_nodes);
    cls_register_cxx_method(h_class, LSM_WRITE_TO_INTERNAL_NODES, CLS_METHOD_RD | CLS_METHOD_WR, lsm_write_to_internal_nodes, &h_lsm_write_to_internal_nodes);
    cls_register_cxx_method(h_class, LSM_CREATE_OBJECT, CLS_METHOD_RD | CLS_METHOD_WR, lsm_create_object, &h_lsm_create_object);
    cls_register_cxx_method(h_class, LSM_PREPARE_COMPACTION, CLS_METHOD_RD | CLS_METHOD_WR, lsm_prepare_compaction, &h_lsm_prepare_compaction);
    cls_register_cxx_method(h_class, LSM_COMPACT, CLS_METHOD_RD | CLS_METHOD_WR, lsm_compact, &h_lsm_compact);
    cls_register_cxx_method(h_class, LSM_UPDATE_POST_COMPACTION, CLS_METHOD_RD | CLS_METHOD_WR, lsm_update_post_compaction, &h_lsm_update_post_compaction);
    cls_register_cxx_method(h_class, LSM_PREPARE_GATHERING, CLS_METHOD_RD | CLS_METHOD_WR, lsm_prepare_gathering, &h_lsm_prepare_gathering);
    cls_register_cxx_method(h_class, LSM_GATHER, CLS_METHOD_RD | CLS_METHOD_WR, lsm_gather, &h_lsm_gather);

    return; 
}