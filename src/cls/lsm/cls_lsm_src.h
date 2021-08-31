#ifndef CEPH_CLS_LSM_SRC_H
#define CEPH_CLS_LSM_SRC_H

#include "objclass/objclass.h"
#include "cls/lsm/cls_lsm_types.h"
#include "cls/lsm/cls_lsm_ops.h"

// reading/writing the node head from/into the object store
int lsm_read_node_head(cls_method_context_t hctx, cls_lsm_node_head& node_head); 
int lsm_write_node_head(cls_method_context_t hctx, cls_lsm_node_head& node_head);
// reading/writing the object data from/into the object store
int lsm_get_entries(cls_method_context_t hctx, cls_lsm_node_head& head, cls_lsm_get_entries_ret& op_ret);
int lsm_append_entries(cls_method_context_t hctx, cls_lsm_append_entries_op& op, cls_lsm_node_head& node);
int lsm_compact_node(cls_method_context_t hctx, cls_lsm_append_entries_op& op, cls_lsm_node_head& head);
// initialize the node with a head and possibly entries
int lsm_init(cls_method_context_t hctx, const cls_lsm_init_op& op);
int lsm_get_child_object_names(cls_method_context_t hctx, cls_lsm_get_child_object_names_ret& op_ret);

#endif /* CEPH_CLS_LSM_SRC_H */