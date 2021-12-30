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
    if (root.size >= root.capacity*4/5) {
        ret = lsm_write_with_compaction(hctx, root, op.entries);        
    } else {
        ret = lsm_write_one_node(hctx, root, entries)
    }
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
        CLS_LOG(1, "ERROR: cls_lsm_write_node: failed reading tree config");
        return ret;
    }

    // check if keys exist in the entire system at all
    std::vector<uint64_t> keys_in_system;
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
        op.keys.insert(op.keys.end(), keys_in_root.begin(), keys_in_root.end());
        ret = lsm_read_data(hctx, root, keys_in_root, op_ret);
    } else {
        op.keys.insert(op.keys.end(), keys_in_system.begin(), keys_in_system.end());
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
    auto it = in->cbegin();
    std::map<std::string, std::vector<uint64_t>> child_read_input;
    try {
        decode(child_read_input, it);
    } catch (const ceph::buffer::error& err) {
        CLS_LOG(1, "ERROR: lsm_read_from_internal_nodes: failed to decode getting input src objects: %s", err.what());
        return -EINVAL;
    }

    cls_lsm_node_head root;
    auto ret = lsm_read_node_head(hctx, root);
    if (ret < 0) {
        return ret;
    }

    ret = cls_cxx_read(hctx, root.entry_start_offset, (root.entry_end_offset - root.entry_start_offset), out);
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
    // Case 5, node exists but is full so needs to invoke compaction
    cls_lsm_node_head old_head;
    auto ret = lsm_read_node_head(hctx, old_head);

    if (ret < 0 || old_head.my_level >= old_head.levels || old_head.size < (old_head.capacity * 4 / 5)) {
        ret = lsm_write_one_node(hctx, new_head, new_entries);
    } else {
        ret = lsm_write_with_compaction(hctx, old_head, new_entries);
    } 

    return ret;
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

    cls_register(LSM_CLASS, &h_class);

    cls_register_cxx_method(h_class, LSM_INIT, CLS_METHOD_RD | CLS_METHOD_WR, cls_lsm_init, &h_lsm_init);
    cls_register_cxx_method(h_class, LSM_WRITE_NODE, CLS_METHOD_RD | CLS_METHOD_WR, cls_lsm_write_node, &h_lsm_write_node);
    cls_register_cxx_method(h_class, LSM_READ_NODE, CLS_METHOD_RD | CLS_METHOD_WR, cls_lsm_read_node, &h_lsm_read_node);
    cls_register_cxx_method(h_class, LSM_READ_FROM_INTERNAL_NODES, CLS_METHOD_RD | CLS_METHOD_WR, cls_lsm_read_from_internal_nodes, &h_lsm_read_from_internal_nodes);
    cls_register_cxx_method(h_class, LSM_WRITE_TO_INTERNAL_NODES, CLS_METHOD_RD | CLS_METHOD_WR, lsm_write_to_internal_nodes, &h_lsm_write_to_internal_nodes);

    return; 
}