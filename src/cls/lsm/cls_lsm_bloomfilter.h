#ifndef CEPH_CLS_LSM_BLOOMFILTER_H
#define CEPH_CLS_LSM_BLOOMFILTER_H

#include "objclass/objclass.h"
#include "cls/lsm/cls_lsm_types.h"

constexpr unsigned int MD5_RESULT_SIZE_BYTES = 16;
constexpr unsigned int HASH_FUNCTION_COUNT = 4;

int lsm_bloomfilter_insert(std::vector<bool>& bloomfilter_store, const std::string& object);
int lsm_bloomfilter_clear(std::vector<bool>& bloomfilter_store);
bool lsm_bloomfilter_contains(std::vector<bool>& bloomfilter_store, const std::string& object);
int lsm_bloomfilter_hash(const std::string& val, std::unique_ptr<unsigned char[]>* MD5_hash_result_buffer);

#endif /* CEPH_CLS_LSM_BLOOMFILTER_H */