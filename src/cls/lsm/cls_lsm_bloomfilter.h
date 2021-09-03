#ifndef CEPH_CLS_LSM_BLOOMFILTER_H
#define CEPH_CLS_LSM_BLOOMFILTER_H

#include "objclass/objclass.h"
#include "cls/lsm/cls_lsm_types.h"

constexpr unsigned int MD5_RESULT_SIZE_BYTES = 16;
constexpr unsigned int HASH_FUNCTION_COUNT = 4;

int lsm_bloomfilter_insert(cls_lsm_node_head& head, const std::string& object);
int lsm_bloomfilter_clear(cls_lsm_node_head& head);
bool lsm_bloomfilter_contains(cls_lsm_node_head& head, const std::string& object);
static int lsm_bloomfilter_hash(const std::string& val, const std::unique_ptr<unsigned char[]> MD5_hash_result_buffer);

#endif /* CEPH_CLS_LSM_BLOOMFILTER_H */