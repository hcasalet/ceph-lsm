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

    int leaf_node_total = (op.key_range.high_bound - op.key_range.low_bound)/op.max_capacity;
    float fan_out_rate = pow(leaf_node_total, 1.0/(float)op.levels);
    int fan_out = static_cast<int>(fan_out_rate) + 1;

    // initialize only the level-1 nodes 
    lsm_init(hctx, op, fan_out);

    return 0;
}

/**
 * write data to node
 */ 
static int cls_lsm_write_node(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    auto iter = in->cbegin();
    cls_lsm_append_entries_op op;
    try {
        decode(op, iter);
    } catch (ceph::buffer::error& err) {
        CLS_LOG(1, "ERROR: cls_lsm_write_node: failed to decode input data \n");
        return -EINVAL;
    }

    cls_lsm_node_head head;
    auto ret = lsm_read_node_head(hctx, head);
    if (ret < 0) {
        return ret;
    }

    if (head.size + op.bl_data_vec.size() > head.max_capacity) {
        return lsm_compact_node(hctx, op, head);
    } else {
        return lsm_append_entries(hctx, op, head);
    }
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

    cls_lsm_node_head node_head;
    auto ret = lsm_read_node_head(hctx, node_head);
    if (ret < 0) {
        return ret;
    }

    // Check the sub-root-node's bloom filter
    ret = lsm_check_if_key_exists(node_head, op);
    if (ret < 0) {
        return ret;
    }
    if (op.keys.size() == 0) {
        CLS_LOG(1, "INFO: keys do not exist in the object store, returning nothing \n");
        return -ENODATA;
    }

    cls_lsm_get_entries_ret op_ret;
    ret = lsm_get_entries(hctx, node_head, op, op_ret);
    if (ret < 0) {
        return ret;
    }

    encode(op_ret, *out);
    return 0;
}

CLS_INIT(lsm)
{
    CLS_LOG(1, "Loaded lsm class!");

    cls_handle_t h_class;
    cls_method_handle_t h_lsm_init;
    cls_method_handle_t h_lsm_write_node;
    cls_method_handle_t h_lsm_read_node;

    cls_register(LSM_CLASS, &h_class);

    cls_register_cxx_method(h_class, LSM_INIT, CLS_METHOD_RD | CLS_METHOD_WR, cls_lsm_init, &h_lsm_init);
    cls_register_cxx_method(h_class, LSM_WRITE_NODE, CLS_METHOD_RD | CLS_METHOD_WR, cls_lsm_write_node, &h_lsm_write_node);
    cls_register_cxx_method(h_class, LSM_READ_NODE, CLS_METHOD_RD | CLS_METHOD_WR, cls_lsm_read_node, &h_lsm_read_node);
    
    return; 
}