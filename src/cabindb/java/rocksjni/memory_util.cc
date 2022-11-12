// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <jni.h>
#include <map>
#include <string>
#include <unordered_set>
#include <vector>

#include "include/org_cabindb_MemoryUtil.h"

#include "cabinjni/portal.h"

#include "cabindb/utilities/memory_util.h"


/*
 * Class:     org_cabindb_MemoryUtil
 * Method:    getApproximateMemoryUsageByType
 * Signature: ([J[J)Ljava/util/Map;
 */
jobject Java_org_cabindb_MemoryUtil_getApproximateMemoryUsageByType(
    JNIEnv *env, jclass, jlongArray jdb_handles, jlongArray jcache_handles) {
  jboolean has_exception = JNI_FALSE;
  std::vector<CABINDB_NAMESPACE::DB *> dbs =
      CABINDB_NAMESPACE::JniUtil::fromJPointers<CABINDB_NAMESPACE::DB>(
          env, jdb_handles, &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  std::unordered_set<const CABINDB_NAMESPACE::Cache *> cache_set;
  jsize cache_handle_count = env->GetArrayLength(jcache_handles);
  if(cache_handle_count > 0) {
    jlong *ptr_jcache_handles = env->GetLongArrayElements(jcache_handles, nullptr);
    if (ptr_jcache_handles == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }
    for (jsize i = 0; i < cache_handle_count; i++) {
      auto *cache_ptr =
          reinterpret_cast<std::shared_ptr<CABINDB_NAMESPACE::Cache> *>(
              ptr_jcache_handles[i]);
      cache_set.insert(cache_ptr->get());
    }
    env->ReleaseLongArrayElements(jcache_handles, ptr_jcache_handles, JNI_ABORT);
  }

  std::map<CABINDB_NAMESPACE::MemoryUtil::UsageType, uint64_t> usage_by_type;
  if (CABINDB_NAMESPACE::MemoryUtil::GetApproximateMemoryUsageByType(
          dbs, cache_set, &usage_by_type) != CABINDB_NAMESPACE::Status::OK()) {
    // Non-OK status
    return nullptr;
  }

  jobject jusage_by_type = CABINDB_NAMESPACE::HashMapJni::construct(
      env, static_cast<uint32_t>(usage_by_type.size()));
  if (jusage_by_type == nullptr) {
    // exception occurred
    return nullptr;
  }
  const CABINDB_NAMESPACE::HashMapJni::FnMapKV<
      const CABINDB_NAMESPACE::MemoryUtil::UsageType, const uint64_t, jobject,
      jobject>
      fn_map_kv = [env](
                      const std::pair<CABINDB_NAMESPACE::MemoryUtil::UsageType,
                                      uint64_t> &pair) {
        // Construct key
        const jobject jusage_type = CABINDB_NAMESPACE::ByteJni::valueOf(
            env, CABINDB_NAMESPACE::MemoryUsageTypeJni::toJavaMemoryUsageType(
                     pair.first));
        if (jusage_type == nullptr) {
          // an error occurred
          return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
        }
        // Construct value
        const jobject jusage_value =
            CABINDB_NAMESPACE::LongJni::valueOf(env, pair.second);
        if (jusage_value == nullptr) {
          // an error occurred
          return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
        }
        // Construct and return pointer to pair of jobjects
        return std::unique_ptr<std::pair<jobject, jobject>>(
            new std::pair<jobject, jobject>(jusage_type,
                                            jusage_value));
      };

  if (!CABINDB_NAMESPACE::HashMapJni::putAll(env, jusage_by_type,
                                             usage_by_type.begin(),
                                             usage_by_type.end(), fn_map_kv)) {
    // exception occcurred
    jusage_by_type = nullptr;
  }

  return jusage_by_type;
}
