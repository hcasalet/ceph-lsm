#ifndef CEPH_CLS_LSM_BLOOMFILTER_H
#define CEPH_CLS_LSM_BLOOMFILTER_H

#include "objclass/objclass.h"
#include "cls/lsm/cls_lsm_types.h"

constexpr unsigned int MD5_RESULT_SIZE_BYTES = 16;
constexpr unsigned int HASH_FUNCTION_COUNT = 4;

int insert(cls_lsm_node_head& head, const std::string& object);
int clear(cls_lsm_node_head& head);
bool contains(cls_lsm_node_head& head, const std::string& object);
static int hash(const std::string& val);

#endif /* CEPH_CLS_LSM_BLOOMFILTER_H */