#ifndef CEPH_CLS_LSM_SRC_H
#define CEPH_CLS_LSM_SRC_H

#include "objclass/objclass.h"
#include "cls/lsm/cls_lsm_types.h"
#include "cls/lsm/cls_lsm_ops.h"

/**
 * function to initialize an lsm tree with its root node on level 0
 */
int lsm_init(cls_method_context_t hctx, const cls_lsm_init_op& op);

/**
 * function to read the config info of a tree stored in root node on level 0
 */
int lsm_read_tree_config(cls_method_context_t hctx, cls_lsm_tree_config& tree);

/**
 * function to write the config info of a tree into the root node on level 0
 */
int lsm_write_tree_config(cls_method_context_t hctx, cls_lsm_tree_config& tree);

/**
 * Read rows whose key matching "keys" (returning only asked columns)
 */
int lsm_read_node(cls_method_context_t hctx, cls_lsm_get_entries_op& op, cls_lsm_get_entries_ret& op_ret);

/**
 * Read rows first from a tree's root node. 
 */
int lsm_read_from_root(cls_method_context_t hctx, cls_lsm_get_entries_op& op, cls_lsm_get_entries_ret& op_ret, std::vector<uint64_t>& found_keys_all);

/**
 * function to read the header info of any lsm tree node on level 1 or higher
 */
int lsm_read_node_head(cls_method_context_t hctx, cls_lsm_node_head& node_head);

/**
 * function to read the data entries stored in an object
 */
int lsm_get_entries(cls_method_context_t hctx, cls_lsm_tree_config& tree, cls_lsm_get_entries_op& op, cls_lsm_get_entries_ret& op_ret, std::vector<uint64_t>& found_keys);

/**
 * function to write data entries into the system
 */
int lsm_write_data(cls_method_context_t hctx, cls_lsm_append_entries_op& op);

/**
 * 
 */
int lsm_write_root_node(cls_method_context_t hctx, cls_lsm_tree_config& tree_config, cls_lsm_entry entry);

/**
 * function to write the header info of any lsm tree node which is on level 1 or higher
 */
int lsm_write_node_head(cls_method_context_t hctx, cls_lsm_node_head& node_head);

/**
 * function to add data entries into an object
 */
int lsm_append_entries(cls_method_context_t hctx, cls_lsm_append_entries_op& op, cls_lsm_node_head& node);

/**
 * function to compact data from a node into its children objects, while writing data into it
 */
int lsm_compact_node(cls_method_context_t hctx, cls_lsm_append_entries_op& op, cls_lsm_node_head& head);

/**
 * function to initialize level 1 nodes
 */
int lsm_init_node(cls_method_context_t hctx, bufferlist *in, bufferlist *out);

/**
 * function to get the children object names
 */
int lsm_get_child_object_names(cls_lsm_node_head& head, cls_lsm_get_entries_op& op, cls_lsm_get_child_object_name_ret& op_ret);

/**
 * function to check if a node has the key being looked for or not
 */
int lsm_check_if_key_exists(std::vector<bool> bloomfilter_store, cls_lsm_get_entries_op& op, std::vector<uint64_t>& found_keys);

#endif /* CEPH_CLS_LSM_SRC_H */