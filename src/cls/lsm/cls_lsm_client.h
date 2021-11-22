#ifndef CEPH_CLS_LSM_CLIENT_H
#define CEPH_CLS_LSM_CLIENT_H

#include "include/rados/librados.hpp"
#include "cls/lsm/cls_lsm_types.h"

/**
 * Initialize the lsm tree, which essentially is to create the root node
 * 
 * Input: 
 * - tree_name: object id of the root node
 * - levels: the number of levels in the tree
 * - key_range: two elements array, one is low bound and the other high bound
 * - fan_out: number of children for a parent node
 * - max_capacity: max number of "rows" in an object
 * - columns: collection of column names
 */
void cls_lsm_init(librados::ObjectWriteOperation& op,
                const std::string& pool_name,
                const std::string& tree_name,
                uint64_t levels,
                std::vector<uint64_t>& key_range,
                uint64_t fan_out,
                uint64_t max_capacity,
                std::vector<std::string>& columns);

/**
 * Read API
 *
 * Input:
 * - oid: object id of the root node in the lsm tree
 * - keys: the collection of keys to be read
 * - columns: the collection of columns to be read
 * Output:
 * - entries: the reading result
 */
int cls_lsm_read(librados::IoCtx& io_ctx, const std::string& oid,
                std::vector<uint64_t>& keys,
                std::vector<std::string>& columns,
                std::vector<cls_lsm_entry>& entries);

/**
 * Write API
 *
 * Input:
 * - oid: object id of the root node to write the data to
 * - bl_data_vec: vector of the "rows" to be written
 */
void cls_lsm_write(librados::ObjectWriteOperation& op,
                const std::string& oid,
                std::vector<cls_lsm_entry> entries);

#endif