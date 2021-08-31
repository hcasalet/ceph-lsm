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
    } catch (cecph::list::error& err) {
        CLS_LOG(1, "ERROR: cls_lsm_init_op: failed to decdoe entry\n");
        return -EINVAL;
    }

    return lsm_init(hctx, op);
}

/**
 * write data to node
 */ 
struct int cls_lsm_write_node(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
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

    if (head.size + cls_lsm_append_entries_op.bl_data_vec.size() > head.max_capacity) {
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
    ret = lsm_read_node_head(hctx, node_head);
    if (ret < 0) {
        return ret;
    }

    cls_lsm_get_entries_ret op_ret;
    ret = lsm_get_entries(hctx, op, op_ret, node_head);
    if (ret < 0) {
        return ret;
    }

    encode(op_ret, *out);
    return 0;
}

/**
 * compaction 
 */ 
static int cls_lsm_compaction(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    auto ret = cls_cxx_scatter_wait_for_completions(hctxx);
    if (ret == -EAGAIN) {
        cls_lsm_node node;
        ret = lsm_read_node(hctx, node);
        if (ret < 0) {
            return ret;
        }

        cls_lsm_retrieve_data_ret data_ret;
        ret = lsm_retrieve_data(hctx, data_ret, node);

        auto iter = in->cbegin();
        cls_lsm_persist_data_op op;
        try {
            decode(op, iter);
        } catch (ceph::buffer::error& err) {
            CLS_LOG(1, "ERROR: cls_lsm_compaction: failed to decode input data \n");
            return -EINVAL;
        }

        cls_lsm_split_data_ret op_ret;
        ret = lsm_compact(hctx, op, data_ret, node, op_ret);

        cls_lsm_get_child_object_names_ret tgt_ret;
        ret = lsm_get_child_object_names(hctx, tgt_ret);

        std::map<std::string, bufferlist> tgt_objs;
        int i = 0;
        for (auto it = tgt_ret.child_object_names.begin(); 
            it != tgt_ret.child_object_names.end() && i < op_ret.split_entries.size(); it++) {
            tgt_objs[it->c_str()] = op_ret.split_entries[i++];
            ret = cls_cxx_scatter(hctx, tgt_objs, pool, "cls_lsm", "cls_lsm_write_node", *in);
        }
    } else {
        if (ret != 0) {
            CLS_ERR("%s: compaction failed. error=%d", __PRETTY_FUNCTION__, ret);
        }
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
    cls_method_handle_t h_lsm_compaction;

    cls_register(LSM_CLASS, &h_class)

    cls_register_cxx_method(h_class, LSM_INIT, CLS_METHOD_RD | CLS_METHOD_WR, cls_lsm_init, &h_lsm_init);
    cls_register_cxx_method(h_class, LSM_WRITE_NODE, CLS_METHOD_RD | CLS_METHOD_WR, cls_lsm_write_node, &h_lsm_write_node);
    cls_register_cxx_method(h_class, LSM_READ_NODE, CLS_METHOD_RD | CLS_METHOD_WR, cls_lsm_read_node, &h_lsm_read_node);
    cls_register_cxx_method(h_class, LSM_COMPACTION, CLS_METHOD_RD | CLS_METHOD_WR, cls_lsm_compaction, &h_lsm_compaction);

    return; 
}