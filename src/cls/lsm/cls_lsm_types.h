#ifndef CEPH_CLS_LSM_TYPES_H
#define CEPH_CLS_LSM_TYPES_H

#include <errno.h>
#include "include/types.h"
#include "objclass/objclass.h"

constexpr unsigned int LSM_NODE_START = 0xDEAD;
constexpr unsigned int LSM_ENTRY_OVERHEAD = sizeof(uint16_t) + sizeof(uint64_t);

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

// naming scheme rules
struct cls_lsm_child_object_naming_map
{
    uint32_t                          key_ranges;
    std::vector<cls_lsm_column_group> clm_groups;

    void encode(ceph::buffer::list& bl) const {
        ENCODE_START(1, 1, bl);
        encode(key_ranges, bl);
        encode(clm_groups, bl);
        ENCODE_FINISH(bl);
    }

    void decode(ceph::buffer::list::const_iterator& bl) {
        DECODE_START(1, bl);
        decode(key_ranges, bl);
        decode(clm_groups, bl);
        DECODE_FINISH(bl);
    }
};
WRITE_CLASS_ENCODER(cls_lsm_child_object_naming_map)

// marker for an entry
struct cls_lsm_marker
{
    uint64_t begin_offset;
    uint64_t end_offset;

    void encode(ceph::buffer::list& bl) const {
        ENCODE_START(1, 1, bl);
        encode(begin_offset, bl);
        encode(end_offset, bl);
        ENCODE_FINISH(bl);
    }
    
    void decode(ceph::buffer::list::const_iterator& bl) {
        DECODE_START(1, bl);
        decode(begin_offset, bl);
        decode(end_offset, bl);
        DECODE_FINISH(bl);
    }
};
WRITE_CLASS_ENCODER(cls_lsm_marker)

// application data stored in the lsm node
// key-value format; value is a map of "column name -> bufferlist"
struct cls_lsm_entry
{
    std::string key;
    std::map<std::string, ceph::buffer::list> value;
    std::string marker;
    
    void encode(ceph::buffer::list& bl) const {
        ENCODE_START(1, 1, bl);
        encode(key, bl);
        encode(value, bl);
        encode(marker, bl);
        ENCODE_FINISH(bl);
    }

    void decode(ceph::buffer::list::const_iterator& bl) {
        DECODE_START(1, bl);
        decode(key, bl);
        decode(value, bl);
        decode(marker, bl);
        DECODE_FINISH(bl);
    }
};
WRITE_CLASS_ENCODER(cls_lsm_entry)

// lsm tree node
struct cls_lsm_node_head
{              
    uint8_t level;                               // level of the tree that the node is on
    cls_lsm_key_range key_range;                 // range of keys stored in this object
    uint64_t max_capacity;                       // max number of objects that can be held
    uint64_t size;                               // number of objects holding already
    uint64_t data_start_offset;                  // marker where data starts
    uint64_t data_end_offset;                    // marker where data ends
    cls_lsm_child_object_naming_map naming_map;  // child node naming map

    void encode(ceph::buffer::list& bl) const {
        ENCODE_START(1, 1, bl);
        encode(level, bl);
        encode(key_range, bl);
        encode(max_capacity, bl);
        encode(size, bl);
        encode(data_start_offset, bl);
        encode(data_end_offset, bl);
        encode(naming_map, bl);
        ENCODE_FINISH(bl);
    }

    void decode(ceph::buffer::list::const_iterator& bl) {
        DECODE_START(1, bl);
        decode(level, bl);
        decode(key_range, bl);
        decode(max_capacity, bl);
        decode(size, bl);
        decode(data_start_offset, bl);
        decode(data_end_offset, bl);
        decode(naming_map, bl);
        DECODE_FINISH(bl);
    }
};
WRITE_CLASS_ENCODER(cls_lsm_node_head)

#endif /* CEPH_CLS_LSM_TYPES_H */
