//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// CABINDB_NAMESPACE::WalFilter.

#ifndef JAVA_CABINJNI_WAL_FILTER_JNICALLBACK_H_
#define JAVA_CABINJNI_WAL_FILTER_JNICALLBACK_H_

#include <jni.h>
#include <map>
#include <memory>
#include <string>

#include "cabindb/wal_filter.h"
#include "cabinjni/jnicallback.h"

namespace CABINDB_NAMESPACE {

class WalFilterJniCallback : public JniCallback, public WalFilter {
 public:
    WalFilterJniCallback(
        JNIEnv* env, jobject jwal_filter);
    virtual void ColumnFamilyLogNumberMap(
        const std::map<uint32_t, uint64_t>& cf_lognumber_map,
        const std::map<std::string, uint32_t>& cf_name_id_map);
    virtual WalFilter::WalProcessingOption LogRecordFound(
        unsigned long long log_number, const std::string& log_file_name,
        const WriteBatch& batch, WriteBatch* new_batch, bool* batch_changed);
    virtual const char* Name() const;

 private:
    std::unique_ptr<const char[]> m_name;
    jmethodID m_column_family_log_number_map_mid;
    jmethodID m_log_record_found_proxy_mid;
};

}  // namespace CABINDB_NAMESPACE

#endif  // JAVA_CABINJNI_WAL_FILTER_JNICALLBACK_H_
