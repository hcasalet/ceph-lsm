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
int lsm_read_data(cls_method_context_t hctx, cls_lsm_node_head& root, cls_lsm_get_entries_op& op, cls_lsm_get_entries_ret& op_ret);

/**
 * Prepare to read from gathering from children objects
 */
int lsm_read_with_gathering(cls_method_context_t hctx, cls_lsm_node_head& root, cls_lsm_get_entries_op& op, cls_lsm_get_entries_ret& op_ret);

/**
 * Do remote reads to gather
 */
int lsm_gather(cls_method_context_t hctx, cls_lsm_node_head root, std::set<std::string>& child_objs, cls_lsm_get_entries_op& op, cls_lsm_get_entries_ret& op_ret);

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
void lsm_check_if_key_exists(std::vector<bool>& bloomfilter_store, std::vector<uint64_t>& search_keys, std::vector<uint64_t>& found_keys);

/**
 * Get Child object ids
 */
int lsm_get_child_object_ids(cls_lsm_node_head& head, std::vector<uint64_t>& keys, std::vector<std::string>& cols, std::set<std::string>& child_objs);

/**
 * Write an entry into an object
 */
int lsm_write_entry(cls_method_context_t hctx, cls_lsm_entry& entry, uint64_t start_offset);

/**
 * Write object including the node head and the data entries
 */
int lsm_write_one_node(cls_method_context_t hctx, cls_lsm_node_head& node_head, std::vector<cls_lsm_entry>& entries);

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
 * Scatter data from a node when it needs to be compacted
 */
int lsm_compact(cls_method_context_t hctx, std::map<std::string, bufferlist> tgt_objects, std::string pool);

/**
 * Find the target object ids to scatter data into
 */
void lsm_get_scatter_targets(cls_lsm_node_head& head, std::vector<cls_lsm_entry>& entries, std::map<std::string, bufferlist>& tgt_child_objects);

/**
 * Make column group splits for children from a parent's splits
 */
std::vector<std::set<std::string>> lsm_make_column_group_splits_for_children(std::set<std::string>& column_splits, int ways);

/**
 * Make data entries for children
 */
std::vector<cls_lsm_entry> lsm_make_data_entries_for_children(std::vector<cls_lsm_entry>& entries, std::set<std::string>& columns);

/**
 * Write with compaction
 */
int lsm_write_with_compaction(cls_method_context_t hctx, cls_lsm_node head root, std::vector<cls_lsm_entry> new_entries);

#endif /* CEPH_CLS_LSM_SRC_H */