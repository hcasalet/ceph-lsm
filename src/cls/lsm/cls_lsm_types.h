#ifndef CEPH_CLS_LSM_TYPES_H
#define CEPH_CLS_LSM_TYPES_H

#include <errno.h>
#include "include/types.h"
#include "objclass/objclass.h"

// size of head
#define LSM_HEAD_SIZE_100K 102400
#define BLOOM_FILTER_STORE_SIZE_64K 65536

constexpr unsigned int LSM_TREE_START = 0xFACE;
constexpr unsigned int LSM_NODE_START = 0xDEAD;
constexpr unsigned int LSM_NODE_OVERHEAD = sizeof(uint16_t) + sizeof(uint64_t);
constexpr unsigned int LSM_ENTRY_START = 0xBEEF;
constexpr unsigned int LSM_PER_KEY_OVERHEAD = sizeof(uint64_t) * 3;
constexpr unsigned int CHUNK_SIZE = 4096;

// key range
struct cls_lsm_key_range
{
    uint64_t low_bound;
    uint64_t high_bound;

    void encode(ceph::buffer::list& bl) const {
        ENCODE_START(1, 1, bl);
        encode(low_bound, bl);
        encode(high_bound, bl);
        ENCODE_FINISH(bl);
    }

    void decode(ceph::buffer::list::const_iterator& bl) {
        DECODE_START(1, bl);
        decode(low_bound, bl);
        decode(high_bound, bl);
        DECODE_FINISH(bl);
    }
};
WRITE_CLASS_ENCODER(cls_lsm_key_range)

// column group
struct cls_lsm_column_group
{
    std::set<std::string> columns;

    void encode(ceph::buffer::list& bl) const {
        ENCODE_START(1, 1, bl);
        encode(columns, bl);
        ENCODE_FINISH(bl);
    }

    void decode(ceph::buffer::list::const_iterator& bl) {
        DECODE_START(1, bl);
        decode(columns, bl);
        DECODE_FINISH(bl);
    }    
};
WRITE_CLASS_ENCODER(cls_lsm_column_group)

// column group map
struct cls_lsm_column_group_map
{
    uint32_t                          key_ranges
    std::map<uint64_t, std::vector<cls_lsm_column_group> > clm_group;

    void encode(ceph::buffer::list& bl) const {
        ENCODE_START(1, 1, bl);
        encode(key_range, bl);
        encode(clm_group, bl);
        ENCODE_FINISH(bl);
    }

    void decode(ceph::buffer::list::const_iterator& bl) {
        DECODE_START(1, bl);
        decode(key_ranges, bl);
        decode(clm_group, bl);
        DECODE_FINISH(bl);
    }
};
WRITE_CLASS_ENCODER(cls_lsm_column_group_map)

// marker for an entry
struct cls_lsm_marker
{
    uint64_t begin_offset;
    uint64_t len;

    void encode(ceph::buffer::list& bl) const {
        ENCODE_START(1, 1, bl);
        encode(begin_offset, bl);
        encode(len, bl);
        ENCODE_FINISH(bl);
    }
    
    void decode(ceph::buffer::list::const_iterator& bl) {
        DECODE_START(1, bl);
        decode(begin_offset, bl);
        decode(len, bl);
        DECODE_FINISH(bl);
    }
};
WRITE_CLASS_ENCODER(cls_lsm_marker)

// application data stored in the lsm node
// key-value format; value is a map of "column name -> bufferlist"
struct cls_lsm_entry
{
    uint64_t key;
    std::map<std::string, ceph::buffer::list> value;
    
    void encode(ceph::buffer::list& bl) const {
        ENCODE_START(1, 1, bl);
        encode(key, bl);
        encode(value, bl);
        ENCODE_FINISH(bl);
    }

    void decode(ceph::buffer::list::const_iterator& bl) {
        DECODE_START(1, bl);
        decode(key, bl);
        decode(value, bl);
        DECODE_FINISH(bl);
    }
};
WRITE_CLASS_ENCODER(cls_lsm_entry)

// lsm tree node
struct cls_lsm_node_head
{          
    std::string pool;                                       // pool in which node resides in
    std::string my_object_id;                               // my own object node id
    uint64_t level;                                         // level of the tree that the node is on
    cls_lsm_key_range key_range;                            // range of keys stored in this object
    uint64_t max_capacity;                                  // max number of objects that can be held
    uint64_t size;                                          // number of objects holding already
    uint64_t entry_start_offset;                            // marker where data starts
    uint64_t entry_end_offset;                              // marker where data ends
    uint64_t key_range_splits;                              // number of pieces key range splits into
    std::vector<std::set<std::string>> column_group_splits; // always splits into two groups
    std::vector<bool>  bloomfilter_store;                   // store for bloomfilter

    void encode(ceph::buffer::list& bl) const {
        ENCODE_START(1, 1, bl);
        encode(pool, bl);
        encode(my_object_id, bl);
        encode(level, bl);
        encode(key_range, bl);
        encode(max_capacity, bl);
        encode(size, bl);
        encode(entry_start_offset, bl);
        encode(entry_end_offset, bl);
        encode(key_range_splits, bl);
        encode(column_group_splits, bl);
        encode(bloomfilter_store, bl);
        ENCODE_FINISH(bl);
    }

    void decode(ceph::buffer::list::const_iterator& bl) {
        DECODE_START(1, bl);
        decode(pool, bl);
        decode(my_object_id, bl);
        decode(level, bl);
        decode(key_range, bl);
        decode(max_capacity, bl);
        decode(size, bl);
        decode(entry_start_offset, bl);
        decode(entry_end_offset, bl);
        decode(key_range_splits, bl);
        decode(column_group_splits, bl);
        decode(bloomfilter_store, bl);
        DECODE_FINISH(bl);
    }
};
WRITE_CLASS_ENCODER(cls_lsm_node_head)

struct cls_lsm_tree_config
{
    std::string pool;
    std::string my_object_id;
    uint64_t    levels;
    cls_lsm_key_range key_range;
    uint64_t    total_columns;
    uint64_t    per_node_capacity;

    void encode(ceph::buffer::list& bl) const {
        ENCODE_START(1, 1, bl);
        encode(pool, bl);
        encode(my_object_id, bl);
        encode(levels, bl);
        encode(key_range, bl);
        encode(total_columns, bl);
        encode(per_node_capacity, bl);
        ENCODE_FINISH(bl);
    }

    void decode(ceph::buffer::list::const_iterator& bl) {
        DECODE_START(1, bl);
        decode(pool, bl);
        decode(my_object_id, bl);
        decode(levels, bl);
        decode(key_range, bl);
        decode(total_columns, bl);
        decode(per_node_capacity, bl);
        DECODE_FINISH(bl);
    }
};
WRITE_CLASS_ENCODER(cls_lsm_tree_config)

#endif /* CEPH_CLS_LSM_TYPES_H */
