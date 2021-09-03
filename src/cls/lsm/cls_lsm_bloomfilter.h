#ifndef CEPH_CLS_LSM_BLOOMFILTER_H
#define CEPH_CLS_LSM_BLOOMFILTER_H

#include "cls/lsm/cls_lsm_types.h"

constexpr unsigned int MD5_RESULT_SIZE_BYTES = 16;
constexpr unsigned int HASH_FUNCTION_COUNT = 4;

void insert(cls_lsm_node_head head, const std::string& object);
void clear(cls_lsm_node_head head);
bool contains(cls_lsm_node_head head, const std::string& object) const;
static void hash(const std::string& val) const;

#endif /* CEPH_CLS_LSM_BLOOMFILTER_H */