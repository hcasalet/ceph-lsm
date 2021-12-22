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
 * Read the config info of a tree stored in root node on level 0. This is part of the initialization.
 */
int lsm_read_tree_config(cls_method_context_t hctx, cls_lsm_tree_config& tree);

/**
 * Write the config info of a tree into the root node on level 0. This is the last part of the initialization.
 */
int lsm_write_tree_config(cls_method_context_t hctx, cls_lsm_tree_config& tree);

/**
 * Read data
 */
int lsm_read_data(cls_method_context_t hctx, cls_lsm_get_entries_op& op, cls_lsm_get_entries_ret& op_ret, std::map<std::string, std::vector<uint64_t>> src_objs_map);

/**
 * Read data first from a tree's root object.
 */
int lsm_read_from_root(cls_method_context_t hctx, cls_lsm_tree_config& tree, cls_lsm_get_entries_op& top, cls_lsm_get_entries_ret& op_ret, std::vector<uint64_t>& found_keys_all);

/**
 * Get data entries from the root object
 */
int lsm_read_entries_from_root(cls_method_context_t hctx, bufferlist *bl_chunk, std::vector<uint64_t>& found_keys, cls_lsm_get_entries_ret& op_ret);

/**
 * Read rows from objects below level 0 (non root objects)
 */
int lsm_read_from_below_level_0(cls_method_context_t hctx, cls_lsm_key_range& key_range, std::vector<std::set<std::string>> column_splits, 
                                std::string parent_id, std::string pool, cls_lsm_get_entries_op& op, cls_lsm_get_entries_ret& op_ret, 
                                std::map<std::string, std::vector<uint64_t>> src_objs_map);

/**
 * function to read the header info of any lsm tree node on level 1 or higher
 */
int lsm_read_from_internal_nodes(cls_method_context_t hctx, std::string pool, std::set<std::string> object_ids, bufferlist* keys_bl, bufferlist* read_ret);

/**
 * Read the node head of a non-root object
 */
int lsm_read_node_head(cls_method_context_t hctx, cls_lsm_node_head& node_head);

/**
 * function to read the data entries stored in an object
 */
int lsm_get_entries(bufferlist *in, std::vector<uint64_t>& read_keys, cls_lsm_get_entries_ret& op_ret);

/**
 * Get the column group names when visiting children objects
 */
int lsm_get_column_groups(std::vector<std::string>& cols, std::vector<std::set<std::string>>& column_group_splits, std::set<uint16_t>& col_grps);

/**
 * function to check if a node has the key being looked for or not
 */
int lsm_check_if_key_exists(std::vector<bool> bloomfilter_store, std::vector<uint64_t>& search_keys, std::vector<uint64_t>& found_keys);

/**
 * Get Child object ids
 */
int lsm_get_child_object_ids(cls_lsm_node_head head, std::vector<uint64_t> keys, cls_lsm_get_entries_op op, std::map<std::string, std::vector<uint64_t>> src_objs_map);

/**
 * function to write data entries into the system
 */
int lsm_write_data(cls_method_context_t hctx, cls_lsm_append_entries_op& op);

/**
 * Write an entry into an object
 */
int lsm_write_entry(cls_method_context_t hctx, cls_lsm_entry& entry, uint64_t start_offset);

/**
 * Write object including the node head and the data entries
 */
int lsm_write_one_node(cls_method_context_t hctx, cls_lsm_node_head& node_head, std::vector<cls_lsm_entry>& entries);

/**
 * This is the function to write, as apposed to compact
 */
int lsm_write_root_node(cls_method_context_t hctx, cls_lsm_tree_config& tree_config, cls_lsm_entry entry);

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
 * function to compact data from a node into its children objects, while writing data into it
 */
int lsm_compact_node(cls_method_context_t hctx, std::vector<cls_lsm_entry>& entries, cls_lsm_node_head& head);

/**
 * Make column group splits for children from a parent's splits
 */
std::vector<std::set<std::string>> lsm_make_column_group_splits_for_children(std::set<std::string>& column_splits, int ways);

/**
 * Make data entries for children
 */
std::vector<cls_lsm_entry> lsm_make_data_entries_for_children(std::vector<cls_lsm_entry> entries, std::set<std::string> columns);

#endif /* CEPH_CLS_LSM_SRC_H */