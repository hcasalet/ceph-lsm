#ifndef CEPH_CLS_LSM_CLIENT_H
#define CEPH_CLS_LSM_CLIENT_H

#include "include/rados/librados.hpp"
#include "cls/lsm/cls_lsm_types.h"

void cls_lsm_init(librados::ObjectWriteOperation& op,
                const std::string& pool_name,
                const std::string& lsm_tree_name,
                uint64_t levels,
                cls_lsm_key_range key_range,
                uint64_t max_capacity,
                std::set<std::string> columns);
int cls_lsm_read(librados::IoCtx& io_ctx, const std::string& oid, 
                std::vector<cls_lsm_entry>& entries,
                std::vector<uint64_t>& keys,
                std::vector<std::string>& columns);
void cls_lsm_write(ObjectWriteOperation& op, 
                std::vector<bufferlist> bl_data_vec);

#endif