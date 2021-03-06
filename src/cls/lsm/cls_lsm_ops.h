// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_LSM_OPS_H
#define CEPH_CLS_LSM_OPS_H

#include "cls/lsm/cls_lsm_types.h"

struct cls_lsm_init_op {
    std::string pool_name;
    std::string obj_name;
    cls_lsm_key_range key_range;

    cls_lsm_init_op() {}

    void encode(ceph::buffer::list& bl) const {
        ENCODE_START(1, 1, bl);
        encode(pool_name, bl);
        encode(obj_name, bl);;
        encode(key_range, bl);
        ENCODE_FINISH(bl);
    }

    void decode(ceph::buffer::list::const_iterator& bl) {
        DECODE_START(1, bl);
        decode(pool_name, bl);
        decode(obj_name, bl); 
        decode(key_range, bl);
        DECODE_FINISH(bl);
    }
};
WRITE_CLASS_ENCODER(cls_lsm_init_op)

struct cls_lsm_append_entries_op {
    std::string tree_name;
    std::vector<cls_lsm_entry> entries;
    //std::map<uint64_t, ceph::buffer::list> bl_data_map;

    cls_lsm_append_entries_op() {}

    void encode (ceph::buffer::list& bl) const {
        ENCODE_START(1, 1, bl);
        encode(tree_name, bl);
        encode(entries, bl);
        ENCODE_FINISH(bl);
    }

    void decode(ceph::buffer::list::const_iterator& bl) {
        DECODE_START(1, bl);
        decode(tree_name, bl);
        decode(entries, bl);
        DECODE_FINISH(bl);
    }
};
WRITE_CLASS_ENCODER(cls_lsm_append_entries_op)

struct cls_lsm_get_entries_op {
    uint64_t key;
    std::vector<std::string> columns;

    cls_lsm_get_entries_op() {}

    void encode(ceph::buffer::list& bl) const {
        ENCODE_START(1, 1, bl);
        encode(key, bl);
        encode(columns, bl);
        ENCODE_FINISH(bl);
    }

    void decode(ceph::buffer::list::const_iterator& bl) {
        DECODE_START(1, bl);
        decode(key, bl);
        decode(columns, bl);
        DECODE_FINISH(bl);
    }
};
WRITE_CLASS_ENCODER(cls_lsm_get_entries_op)

struct cls_lsm_column_groups_name_ret {
    std::set<int> column_groups;

    cls_lsm_column_groups_name_ret() {}

    void encode(ceph::buffer::list& bl) const {
        ENCODE_START(1, 1, bl);
        encode(column_groups, bl);
        ENCODE_FINISH(bl);
    }

    void decode(ceph::buffer::list::const_iterator& bl) {
        DECODE_START(1, bl);
        decode(column_groups, bl);
        DECODE_FINISH(bl);
    }
};
WRITE_CLASS_ENCODER(cls_lsm_column_groups_name_ret)

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

struct cls_lsm_read_from_children_ret {
    std::map<std::string, bufferlist> read_map;

    cls_lsm_read_from_children_ret() {}

    void encode (ceph::buffer::list& bl) const {
        ENCODE_START(1, 1, bl);
        encode(read_map, bl);
        ENCODE_FINISH(bl);
    }

    void decode(ceph::buffer::list::const_iterator& bl) {
        DECODE_START(1, bl);
        decode(read_map, bl);
        DECODE_FINISH(bl);
    }
};
WRITE_CLASS_ENCODER(cls_lsm_read_from_children_ret)

struct cls_lsm_get_child_object_name_ret {
    std::set<std::string> child_object_name;

    cls_lsm_get_child_object_name_ret() {}

    void encode (ceph::buffer::list& bl) const {
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
        DECODE_START(1, bl);
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
