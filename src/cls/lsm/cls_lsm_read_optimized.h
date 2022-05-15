#ifndef CEPH_CLS_READ_OPTIMIZED_CLIENT_H
#define CEPH_CLS_READ_OPTIMIZED_CLIENT_H

#include "include/rados/librados.hpp"
#include "cls/lsm/cls_lsm_types.h"
#include <iostream>
#include <string>

class ClsReadOptimizedClient {

    typedef std::map<int, std::vector<std::vector<bool>>> BloomfilterStore;

public:
    ClsReadOptimizedClient() {};

    void InitClient(std::string tree, uint64_t key_low, uint64_t key_high, int splits, int levels, 
            std::map<int, std::vector<std::vector<std::string>>>& col_map);
 
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
    void cls_read_optimized_init(librados::ObjectWriteOperation& op,
                    const std::string& pool_name,
                    const std::string& tree_name,
                    cls_lsm_key_range& key_range);

    /**
    * Read API
    *
    * Input:
    * - io_ctx: the Input/Output context of Ceph
    * - key: the key whose value is to be read
    * - columns: the collection of columns to be read
    * Output:
    * - entry: the value of the found key
    */
    int cls_read_optimized_read(librados::IoCtx& io_ctx,
                    const std::string& pool_name,
                    uint64_t key,
                    const std::vector<std::string> *columns,
                    cls_lsm_entry& entry);

    /**
    * Write API
    *
    * Input:
    * - oid: object id of the root node to write the data to
    * - bl_data_vec: vector of the "rows" to be written
    */
    void cls_read_optimized_write(librados::IoCtx& io_ctx, const std::string& oid, cls_lsm_entry& entry);
    
    /**
    * Compact API
    * 
    * Input: 
    * - oid: object id of the node to be compacted
    */
    int cls_read_optimized_compact(librados::IoCtx& io_ctx, const std::string& oid);

    /**
    * Gather API
    * 
    * Input: 
    * - oid: object id of the node to be compacted
    * - keys: the collection of keys to be read
    * - columns: the collection of columns to be read
    */
    int cls_read_optimized_scan(librados::IoCtx& io_ctx,
                 uint64_t start_key, uint64_t max_key,
                 std::vector<std::string>& columns,
                 std::vector<cls_lsm_entry>& entries);

private:
    std::string tree_name;
    uint64_t key_low_bound;
    uint64_t key_high_bound;
    int            key_splits;
    int            levels;
    BloomfilterStore bloomfilter_store;
    std::map<int, std::vector<std::vector<std::string>>> column_map;

    int update_bloomfilter(bufferlist in, int level);
};

#endif
