// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <jni.h>

#include "include/org_cabindb_CabinDBExceptionTest.h"

#include "cabindb/slice.h"
#include "cabindb/status.h"
#include "cabinjni/portal.h"

/*
 * Class:     org_cabindb_CabinDBExceptionTest
 * Method:    raiseException
 * Signature: ()V
 */
void Java_org_cabindb_CabinDBExceptionTest_raiseException(JNIEnv* env,
                                                          jobject /*jobj*/) {
  CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env,
                                                   std::string("test message"));
}

/*
 * Class:     org_cabindb_CabinDBExceptionTest
 * Method:    raiseExceptionWithStatusCode
 * Signature: ()V
 */
void Java_org_cabindb_CabinDBExceptionTest_raiseExceptionWithStatusCode(
    JNIEnv* env, jobject /*jobj*/) {
  CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(
      env, "test message", CABINDB_NAMESPACE::Status::NotSupported());
}

/*
 * Class:     org_cabindb_CabinDBExceptionTest
 * Method:    raiseExceptionNoMsgWithStatusCode
 * Signature: ()V
 */
void Java_org_cabindb_CabinDBExceptionTest_raiseExceptionNoMsgWithStatusCode(
    JNIEnv* env, jobject /*jobj*/) {
  CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(
      env, CABINDB_NAMESPACE::Status::NotSupported());
}

/*
 * Class:     org_cabindb_CabinDBExceptionTest
 * Method:    raiseExceptionWithStatusCodeSubCode
 * Signature: ()V
 */
void Java_org_cabindb_CabinDBExceptionTest_raiseExceptionWithStatusCodeSubCode(
    JNIEnv* env, jobject /*jobj*/) {
  CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(
      env, "test message",
      CABINDB_NAMESPACE::Status::TimedOut(
          CABINDB_NAMESPACE::Status::SubCode::kLockTimeout));
}

/*
 * Class:     org_cabindb_CabinDBExceptionTest
 * Method:    raiseExceptionNoMsgWithStatusCodeSubCode
 * Signature: ()V
 */
void Java_org_cabindb_CabinDBExceptionTest_raiseExceptionNoMsgWithStatusCodeSubCode(
    JNIEnv* env, jobject /*jobj*/) {
  CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(
      env, CABINDB_NAMESPACE::Status::TimedOut(
               CABINDB_NAMESPACE::Status::SubCode::kLockTimeout));
}

/*
 * Class:     org_cabindb_CabinDBExceptionTest
 * Method:    raiseExceptionWithStatusCodeState
 * Signature: ()V
 */
void Java_org_cabindb_CabinDBExceptionTest_raiseExceptionWithStatusCodeState(
    JNIEnv* env, jobject /*jobj*/) {
  CABINDB_NAMESPACE::Slice state("test state");
  CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(
      env, "test message", CABINDB_NAMESPACE::Status::NotSupported(state));
}
