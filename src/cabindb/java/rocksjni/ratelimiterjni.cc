// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for RateLimiter.

#include "include/org_cabindb_RateLimiter.h"
#include "cabindb/rate_limiter.h"
#include "cabinjni/portal.h"

/*
 * Class:     org_cabindb_RateLimiter
 * Method:    newRateLimiterHandle
 * Signature: (JJIBZ)J
 */
jlong Java_org_cabindb_RateLimiter_newRateLimiterHandle(
    JNIEnv* /*env*/, jclass /*jclazz*/, jlong jrate_bytes_per_second,
    jlong jrefill_period_micros, jint jfairness, jbyte jrate_limiter_mode,
    jboolean jauto_tune) {
  auto rate_limiter_mode =
      CABINDB_NAMESPACE::RateLimiterModeJni::toCppRateLimiterMode(
          jrate_limiter_mode);
  auto* sptr_rate_limiter = new std::shared_ptr<CABINDB_NAMESPACE::RateLimiter>(
      CABINDB_NAMESPACE::NewGenericRateLimiter(
          static_cast<int64_t>(jrate_bytes_per_second),
          static_cast<int64_t>(jrefill_period_micros),
          static_cast<int32_t>(jfairness), rate_limiter_mode, jauto_tune));

  return reinterpret_cast<jlong>(sptr_rate_limiter);
}

/*
 * Class:     org_cabindb_RateLimiter
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_cabindb_RateLimiter_disposeInternal(JNIEnv* /*env*/,
                                                  jobject /*jobj*/,
                                                  jlong jhandle) {
  auto* handle =
      reinterpret_cast<std::shared_ptr<CABINDB_NAMESPACE::RateLimiter>*>(
          jhandle);
  delete handle;  // delete std::shared_ptr
}

/*
 * Class:     org_cabindb_RateLimiter
 * Method:    setBytesPerSecond
 * Signature: (JJ)V
 */
void Java_org_cabindb_RateLimiter_setBytesPerSecond(JNIEnv* /*env*/,
                                                    jobject /*jobj*/,
                                                    jlong handle,
                                                    jlong jbytes_per_second) {
  reinterpret_cast<std::shared_ptr<CABINDB_NAMESPACE::RateLimiter>*>(handle)
      ->get()
      ->SetBytesPerSecond(jbytes_per_second);
}

/*
 * Class:     org_cabindb_RateLimiter
 * Method:    getBytesPerSecond
 * Signature: (J)J
 */
jlong Java_org_cabindb_RateLimiter_getBytesPerSecond(JNIEnv* /*env*/,
                                                     jobject /*jobj*/,
                                                     jlong handle) {
  return reinterpret_cast<std::shared_ptr<CABINDB_NAMESPACE::RateLimiter>*>(
             handle)
      ->get()
      ->GetBytesPerSecond();
}

/*
 * Class:     org_cabindb_RateLimiter
 * Method:    request
 * Signature: (JJ)V
 */
void Java_org_cabindb_RateLimiter_request(JNIEnv* /*env*/, jobject /*jobj*/,
                                          jlong handle, jlong jbytes) {
  reinterpret_cast<std::shared_ptr<CABINDB_NAMESPACE::RateLimiter>*>(handle)
      ->get()
      ->Request(jbytes, CABINDB_NAMESPACE::Env::IO_TOTAL);
}

/*
 * Class:     org_cabindb_RateLimiter
 * Method:    getSingleBurstBytes
 * Signature: (J)J
 */
jlong Java_org_cabindb_RateLimiter_getSingleBurstBytes(JNIEnv* /*env*/,
                                                       jobject /*jobj*/,
                                                       jlong handle) {
  return reinterpret_cast<std::shared_ptr<CABINDB_NAMESPACE::RateLimiter>*>(
             handle)
      ->get()
      ->GetSingleBurstBytes();
}

/*
 * Class:     org_cabindb_RateLimiter
 * Method:    getTotalBytesThrough
 * Signature: (J)J
 */
jlong Java_org_cabindb_RateLimiter_getTotalBytesThrough(JNIEnv* /*env*/,
                                                        jobject /*jobj*/,
                                                        jlong handle) {
  return reinterpret_cast<std::shared_ptr<CABINDB_NAMESPACE::RateLimiter>*>(
             handle)
      ->get()
      ->GetTotalBytesThrough();
}

/*
 * Class:     org_cabindb_RateLimiter
 * Method:    getTotalRequests
 * Signature: (J)J
 */
jlong Java_org_cabindb_RateLimiter_getTotalRequests(JNIEnv* /*env*/,
                                                    jobject /*jobj*/,
                                                    jlong handle) {
  return reinterpret_cast<std::shared_ptr<CABINDB_NAMESPACE::RateLimiter>*>(
             handle)
      ->get()
      ->GetTotalRequests();
}
