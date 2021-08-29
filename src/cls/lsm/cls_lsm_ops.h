// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_LSM_OPS_H
#define CEPH_CLS_LSM_OPS_H

#include "cls/lsm/cls_lsm_types.h"

struct cls_lsm_init_op {
    uint8_t level;
    cls_lsm_key_range key_range;
    uint64_t max_capacity;
    uint64_t size;
    uint64_t data_start_offset;
    uint64_t data_end_offset;
    cls_lsm_child_object_naming_map naming_map;
    std::vector<cls_lsm_entry> object_data;

    cls_lsm_init_op() {}

    void encode(ceph::buffer::list& bl) const {
        ENCODE_START(1, 1, bl);
        encode(level, bl);
        encode(key_range, bl);
        encode(max_capacity, bl);
        encode(size, bl);
        encode(data_start_offset, bl);
        encode(data_end_offset, bl);
        encode(naming_map, bl);
        encode(object_data, bl);
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
        decode(object_data, bl);
        DECODE_FINISH(bl);
    }
};
WRITE_CLASS_ENCODER(cls_lsm_init_op)

struct cls_lsm_append_entries_op {
    std::vector<cls_lsm_entry> bl_data_vec;

    cls_lsm_append_entries_op() {}

    void encode (ceph::buffer::list& bl) const {
        ENCODE_START(1, 1, bl);
        encode(bl_data_vec, bl);
        ENCODE_FINISH(bl);
    }

    void decode(ceph::buffer::list::const_iterator& bl) {
        DECODE_START(1, bl);
        decode(bl_data_vec, bl);
        DECODE_FINISH(bl);
    }
};
WRITE_CLASS_ENCODER(cls_lsm_append_entries_op)

struct cls_lsm_get_entries_op {
    cls_lsm_marker marker;

    cls_lsm_get_entries_op() {}

    void encode(ceph::buffer::list& bl) const {
        ENCODE_START(1, 1, bl);
        encode(marker, bl);
        ENCODE_FINISH(bl);
    }

    void decode(ceph::buffer::list::const_iterator& bl) {
        DECODE_START(1, bl);
        decode(marker, bl);
    }
};
WRITE_CLASS_ENCODER(cls_lsm_get_entries_op)

struct cls_lsm_get_entries_ret {
    std::vector<cls_lsm_entry> entries;

    cls_lsm_get_entries_ret() {}

    void encode(ceph::buffer::list& bl) const {
        ENCODE_START(1, 1, bl);
        encode(entries, bl);
        ENCODE_FINISH(bl);
    }

    void decode(ceph::buffer::list::const_iterator& bl) {
        DECODE_START(1, bl);
        decode(entries, bl);
        DECODE_FINISH(bl);
    }
};
WRITE_CLASS_ENCODER(cls_lsm_get_entries_ret)

struct cls_lsm_get_child_object_names_ret {
    std::vector<std::string> child_object_names;

    cls_lsm_get_child_object_names_ret() {}

    void encode (ceph::buffer::list& bl) const {
        ENCODE_START(1, 1, bl);
        encode(child_object_names, bl);
        ENCODE_FINISH(bl);
    }

    void decode(ceph::buffer::list::const_iterator& bl) {
        DECODE_START(1, bl);
        decode(child_object_names, bl);
        DECODE_FINISH(bl);
    }
};
WRITE_CLASS_ENCODER(cls_lsm_get_child_object_names_ret)

struct cls_lsm_get_child_object_name_ret {
    std::string child_object_name;

    cls_lsm_get_child_object_name_ret() {}

    void encode (cecph::buffer::list& bl) const {
        ENCODE_START(1, 1, bl);
        encode(child_object_name, bl);
        ENCODE_FINISH(bl);
    }

    void decode(ceph::buffer::list::const_iterator& bl) {
        DECODE_START(1, bl);
        decode(child_object_name, bl);
        DECODE_FINISH(bl);
    }
};
WRITE_CLASS_ENCODER(cls_lsm_get_child_object_name_ret)

struct cls_lsm_retrieve_data_op {
    uint64_t start_marker;

    cls_lsm_retrieve_data_op() {}

    void encode(ceph::buffer::list& bl) const {
        ENCODE_START(1, 1, bl);
        encode(start_marker, bl);
        ENCODE_FINISH(bl);
    }

    void decode(ceph::buffer::list::const_iterator& bl) {
        DECODE_START(1, 1, bl);
        decode(start_marker, bl);
        DECODE_FINISH(bl);
    }
};
WRITE_CLASS_ENCODER(cls_lsm_retrieve_data_op)

struct cls_lsm_split_data_ret {
    std::vector<std::vector<cls_lsm_entry>> split_entries;

    cls_lsm_split_data_ret() {}

    void encode(ceph::buffer::list& bl) const {
        ENCODE_START(1, 1, bl);
        encode(split_entries, bl);
        ENCODE_FINISH(bl);
    }

    void decode(ceph::buffer::list::const_iterator& bl) {
        DECODE_START(1, bl);
        decode(split_entries, bl);
        DECODE_FINISH(bl);
    }
};
WRITE_CLASS_ENCODER(cls_lsm_split_data_ret)

struct cls_lsm_compact_op {
    std::vector<cls_lsm_entry> bl_data_vec;

    cls_lsm_compact_op() {}

    void encode(ceph::buffer::list& bl) const {
        ENCODE_START(1, 1, bl);
        encode(bl_data_vec, bl);
        ENCODE_FINISH(bl);
    }

    void decode(ceph::buffer::list::const_iterator& bl) {
        DECODE_START(1, bl);
        decode(bl_data_vec, bl);
        DECODE_FINISH(bl);
    }
};
WRITE_CLASS_ENCODER(cls_lsm_compact_op)

#endif /* CEPH_CLS_LSM_OPS_H */
