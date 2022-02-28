#ifndef CEPH_CLS_LSM_SRC_H
#define CEPH_CLS_LSM_SRC_H

#include "objclass/objclass.h"
#include "cls/lsm/cls_lsm_types.h"
#include "cls/lsm/cls_lsm_ops.h"

/**
 * Initialize an lsm tree with its root node on level 0
 */
int lsm_init(cls_method_context_t hctx, const cls_lsm_init_op& op);

/**
 * Read data
 */
int lsm_read_data(cls_method_context_t hctx, uint64_t key, cls_lsm_entry& entry);

/**
 * Read the node head of a non-root object
 */
int lsm_read_node_head(cls_method_context_t hctx, cls_lsm_node_head& node_head);

/**
 * function to read the data entries stored in an object
 */
int lsm_get_entries(bufferlist *in, std::vector<cls_lsm_entry>& entries);

/**
 * Write object including the node head and the data entry
 */
int lsm_write_entries(cls_method_context_t hctx, std::vector<cls_lsm_entry>& entries);

/**
 * Write object including the node head and the data entries
 */
int lsm_write_entries(cls_method_context_t hctx, cls_lsm_node_head& head, std::vector<cls_lsm_entry>& entries);

/**
 * Function to compact among internal nodes
 */
int lsm_write_internal_nodes(cls_method_context_t hctx, bufferlist *in, bufferlist *out);

/**
 * function to write the header info of any lsm tree node which is on level 1 or higher
 */
int lsm_write_node_head(cls_method_context_t hctx, cls_lsm_node_head& node_head);

/**
 * function to add data entries into an object
 */
int lsm_append_entries(cls_method_context_t hctx, cls_lsm_append_entries_op& op, cls_lsm_node_head& node);

/**
 * Find the target object ids to scatter data into
 */
void lsm_get_scatter_targets(cls_lsm_node_head& head,
                            std::vector<std::vector<cls_lsm_entry>>& split_entries,
                            std::map<std::string, bufferlist>& tgt_child_objects);

/**
 * Make column group splits for children from a parent's splits
 */
std::vector<std::set<std::string>> lsm_make_column_group_splits_for_children(std::set<std::string>& column_splits, int ways);

/**
 * Make data entries for children
 */
std::vector<cls_lsm_entry> lsm_make_data_entries_for_children(std::vector<cls_lsm_entry>& entries, std::set<std::string>& columns);

#endif /* CEPH_CLS_LSM_SRC_H */