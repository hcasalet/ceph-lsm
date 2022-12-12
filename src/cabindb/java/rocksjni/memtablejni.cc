// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for MemTables.

#include "include/org_cabindb_HashLinkedListMemTableConfig.h"
#include "include/org_cabindb_HashSkipListMemTableConfig.h"
#include "include/org_cabindb_SkipListMemTableConfig.h"
#include "include/org_cabindb_VectorMemTableConfig.h"
#include "cabindb/memtablerep.h"
#include "cabinjni/portal.h"

/*
 * Class:     org_cabindb_HashSkipListMemTableConfig
 * Method:    newMemTableFactoryHandle
 * Signature: (JII)J
 */
jlong Java_org_cabindb_HashSkipListMemTableConfig_newMemTableFactoryHandle(
    JNIEnv* env, jobject /*jobj*/, jlong jbucket_count, jint jheight,
    jint jbranching_factor) {
  CABINDB_NAMESPACE::Status s =
      CABINDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(jbucket_count);
  if (s.ok()) {
    return reinterpret_cast<jlong>(CABINDB_NAMESPACE::NewHashSkipListRepFactory(
        static_cast<size_t>(jbucket_count), static_cast<int32_t>(jheight),
        static_cast<int32_t>(jbranching_factor)));
  }
  CABINDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  return 0;
}

/*
 * Class:     org_cabindb_HashLinkedListMemTableConfig
 * Method:    newMemTableFactoryHandle
 * Signature: (JJIZI)J
 */
jlong Java_org_cabindb_HashLinkedListMemTableConfig_newMemTableFactoryHandle(
    JNIEnv* env, jobject /*jobj*/, jlong jbucket_count,
    jlong jhuge_page_tlb_size, jint jbucket_entries_logging_threshold,
    jboolean jif_log_bucket_dist_when_flash, jint jthreshold_use_skiplist) {
  CABINDB_NAMESPACE::Status statusBucketCount =
      CABINDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(jbucket_count);
  CABINDB_NAMESPACE::Status statusHugePageTlb =
      CABINDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(
          jhuge_page_tlb_size);
  if (statusBucketCount.ok() && statusHugePageTlb.ok()) {
    return reinterpret_cast<jlong>(CABINDB_NAMESPACE::NewHashLinkListRepFactory(
        static_cast<size_t>(jbucket_count),
        static_cast<size_t>(jhuge_page_tlb_size),
        static_cast<int32_t>(jbucket_entries_logging_threshold),
        static_cast<bool>(jif_log_bucket_dist_when_flash),
        static_cast<int32_t>(jthreshold_use_skiplist)));
  }
  CABINDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(
      env, !statusBucketCount.ok() ? statusBucketCount : statusHugePageTlb);
  return 0;
}

/*
 * Class:     org_cabindb_VectorMemTableConfig
 * Method:    newMemTableFactoryHandle
 * Signature: (J)J
 */
jlong Java_org_cabindb_VectorMemTableConfig_newMemTableFactoryHandle(
    JNIEnv* env, jobject /*jobj*/, jlong jreserved_size) {
  CABINDB_NAMESPACE::Status s =
      CABINDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(jreserved_size);
  if (s.ok()) {
    return reinterpret_cast<jlong>(new CABINDB_NAMESPACE::VectorRepFactory(
        static_cast<size_t>(jreserved_size)));
  }
  CABINDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  return 0;
}

/*
 * Class:     org_cabindb_SkipListMemTableConfig
 * Method:    newMemTableFactoryHandle0
 * Signature: (J)J
 */
jlong Java_org_cabindb_SkipListMemTableConfig_newMemTableFactoryHandle0(
    JNIEnv* env, jobject /*jobj*/, jlong jlookahead) {
  CABINDB_NAMESPACE::Status s =
      CABINDB_NAMESPACE::JniUtil::check_if_jlong_fits_size_t(jlookahead);
  if (s.ok()) {
    return reinterpret_cast<jlong>(new CABINDB_NAMESPACE::SkipListFactory(
        static_cast<size_t>(jlookahead)));
  }
  CABINDB_NAMESPACE::IllegalArgumentExceptionJni::ThrowNew(env, s);
  return 0;
}