#ifndef CEPH_CLS_LSM_TYPES_H
#define CEPH_CLS_LSM_TYPES_H

#include <errno.h>
#include "include/types.h"
#include "objclass/objclass.h"

/**
 * The root node of the lsm tree has two bloomfilter stores: the bloomfilter_store_all
 * which uses the BLOOM_FILTER_STORE_SIZE_256K to keep the existence of all data ever
 * written into the system; and the bloomfilter_store_root which uses the BLOOM_FILTER_STORE_SIZE_64K
 * to keep track of the data that the root node currently has.
 *
 * All the other nodes in the lsm tree use the BLOOM_FILTER_STORE_SIZE_64K to keep track of
 * the data that they currently hold.
 */
#define BLOOM_FILTER_STORE_SIZE_64K 65536
#define BLOOM_FILTER_STORE_SIZE_256K 262144

/**
 * padding buffer between the tree-config in root node and the data
 */
#define LSM_DATA_START_PADDING 49152

// size of head
#define LSM_NON_ROOT_DATA_START_100K 102400

// number of ways to split column group for each compaction
#define LSM_COLUMN_SPLIT_FACTOR 2

constexpr unsigned int LSM_TREE_START = 0xFACE;
constexpr unsigned int LSM_NODE_START = 0xDEAD;
constexpr unsigned int LSM_NODE_OVERHEAD = sizeof(uint16_t) + sizeof(uint64_t);
constexpr unsigned int LSM_ENTRY_START = 0xBEEF;
constexpr unsigned int LSM_PER_KEY_OVERHEAD = sizeof(uint64_t) * 3;
constexpr unsigned int CHUNK_SIZE = 102400;

// key range
struct cls_lsm_key_range
{
    uint64_t low_bound;
    uint64_t high_bound;
    int      splits;

    void encode(ceph::buffer::list& bl) const {
        ENCODE_START(1, 1, bl);
        encode(low_bound, bl);
        encode(high_bound, bl);
        encode(splits, bl);
        ENCODE_FINISH(bl);
    }

    void decode(ceph::buffer::list::const_iterator& bl) {
        DECODE_START(1, bl);
        decode(low_bound, bl);
        decode(high_bound, bl);
        decode(splits, bl);
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
    uint32_t                          key_range;
    std::map<uint64_t, std::vector<cls_lsm_column_group> > clm_group;

    void encode(ceph::buffer::list& bl) const {
        ENCODE_START(1, 1, bl);
        encode(key_range, bl);
        encode(clm_group, bl);
        ENCODE_FINISH(bl);
    }

    void decode(ceph::buffer::list::const_iterator& bl) {
        DECODE_START(1, bl);
        decode(key_range, bl);
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
    std::string object_id;                                     // my own object node id
    std::string pool;                                          // pool in which the object is
    cls_lsm_key_range key_range;                               // range of keys stored in this object
    uint64_t size;                                             // number of objects holding already
    std::map<uint64_t, std::pair<uint64_t, uint64_t>> key_map; // location of each key/value pair
    uint64_t data_start_offset;                                // marker where app data starts
    uint64_t data_end_offset;                                  // tail of the data

    void encode(ceph::buffer::list& bl) const {
        ENCODE_START(1, 1, bl);
        encode(object_id, bl);
        encode(pool, bl);
        encode(key_range, bl);
        encode(size, bl);
        encode(key_map, bl);
        encode(data_start_offset, bl);
        encode(data_end_offset, bl);
        ENCODE_FINISH(bl);
    }

    void decode(ceph::buffer::list::const_iterator& bl) {
        DECODE_START(1, bl);
        decode(object_id, bl);
        decode(pool, bl);
        decode(key_range, bl);
        decode(size, bl);
        decode(key_map, bl);
        decode(data_start_offset, bl);
        decode(data_end_offset, bl);
        DECODE_FINISH(bl);
    }
};
WRITE_CLASS_ENCODER(cls_lsm_node_head)

#endif /* CEPH_CLS_LSM_TYPES_H */
