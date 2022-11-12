// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ CABINDB_NAMESPACE::Env methods from Java side.

#include <jni.h>
#include <vector>

#include "portal.h"
#include "cabindb/env.h"
#include "include/org_cabindb_Env.h"
#include "include/org_cabindb_HdfsEnv.h"
#include "include/org_cabindb_CabinEnv.h"
#include "include/org_cabindb_CabinMemEnv.h"
#include "include/org_cabindb_TimedEnv.h"

/*
 * Class:     org_cabindb_Env
 * Method:    getDefaultEnvInternal
 * Signature: ()J
 */
jlong Java_org_cabindb_Env_getDefaultEnvInternal(
    JNIEnv*, jclass) {
  return reinterpret_cast<jlong>(CABINDB_NAMESPACE::Env::Default());
}

/*
 * Class:     org_cabindb_CabinEnv
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_cabindb_CabinEnv_disposeInternal(
    JNIEnv*, jobject, jlong jhandle) {
  auto* e = reinterpret_cast<CABINDB_NAMESPACE::Env*>(jhandle);
  assert(e != nullptr);
  delete e;
}

/*
 * Class:     org_cabindb_Env
 * Method:    setBackgroundThreads
 * Signature: (JIB)V
 */
void Java_org_cabindb_Env_setBackgroundThreads(
    JNIEnv*, jobject, jlong jhandle, jint jnum, jbyte jpriority_value) {
  auto* cabin_env = reinterpret_cast<CABINDB_NAMESPACE::Env*>(jhandle);
  cabin_env->SetBackgroundThreads(
      static_cast<int>(jnum),
      CABINDB_NAMESPACE::PriorityJni::toCppPriority(jpriority_value));
}

/*
 * Class:     org_cabindb_Env
 * Method:    getBackgroundThreads
 * Signature: (JB)I
 */
jint Java_org_cabindb_Env_getBackgroundThreads(
    JNIEnv*, jobject, jlong jhandle, jbyte jpriority_value) {
  auto* cabin_env = reinterpret_cast<CABINDB_NAMESPACE::Env*>(jhandle);
  const int num = cabin_env->GetBackgroundThreads(
      CABINDB_NAMESPACE::PriorityJni::toCppPriority(jpriority_value));
  return static_cast<jint>(num);
}

/*
 * Class:     org_cabindb_Env
 * Method:    getThreadPoolQueueLen
 * Signature: (JB)I
 */
jint Java_org_cabindb_Env_getThreadPoolQueueLen(
    JNIEnv*, jobject, jlong jhandle, jbyte jpriority_value) {
  auto* cabin_env = reinterpret_cast<CABINDB_NAMESPACE::Env*>(jhandle);
  const int queue_len = cabin_env->GetThreadPoolQueueLen(
      CABINDB_NAMESPACE::PriorityJni::toCppPriority(jpriority_value));
  return static_cast<jint>(queue_len);
}

/*
 * Class:     org_cabindb_Env
 * Method:    incBackgroundThreadsIfNeeded
 * Signature: (JIB)V
 */
void Java_org_cabindb_Env_incBackgroundThreadsIfNeeded(
    JNIEnv*, jobject, jlong jhandle, jint jnum, jbyte jpriority_value) {
  auto* cabin_env = reinterpret_cast<CABINDB_NAMESPACE::Env*>(jhandle);
  cabin_env->IncBackgroundThreadsIfNeeded(
      static_cast<int>(jnum),
      CABINDB_NAMESPACE::PriorityJni::toCppPriority(jpriority_value));
}

/*
 * Class:     org_cabindb_Env
 * Method:    lowerThreadPoolIOPriority
 * Signature: (JB)V
 */
void Java_org_cabindb_Env_lowerThreadPoolIOPriority(
    JNIEnv*, jobject, jlong jhandle, jbyte jpriority_value) {
  auto* cabin_env = reinterpret_cast<CABINDB_NAMESPACE::Env*>(jhandle);
  cabin_env->LowerThreadPoolIOPriority(
      CABINDB_NAMESPACE::PriorityJni::toCppPriority(jpriority_value));
}

/*
 * Class:     org_cabindb_Env
 * Method:    lowerThreadPoolCPUPriority
 * Signature: (JB)V
 */
void Java_org_cabindb_Env_lowerThreadPoolCPUPriority(
    JNIEnv*, jobject, jlong jhandle, jbyte jpriority_value) {
  auto* cabin_env = reinterpret_cast<CABINDB_NAMESPACE::Env*>(jhandle);
  cabin_env->LowerThreadPoolCPUPriority(
      CABINDB_NAMESPACE::PriorityJni::toCppPriority(jpriority_value));
}

/*
 * Class:     org_cabindb_Env
 * Method:    getThreadList
 * Signature: (J)[Lorg/cabindb/ThreadStatus;
 */
jobjectArray Java_org_cabindb_Env_getThreadList(
    JNIEnv* env, jobject, jlong jhandle) {
  auto* cabin_env = reinterpret_cast<CABINDB_NAMESPACE::Env*>(jhandle);
  std::vector<CABINDB_NAMESPACE::ThreadStatus> thread_status;
  CABINDB_NAMESPACE::Status s = cabin_env->GetThreadList(&thread_status);
  if (!s.ok()) {
    // error, throw exception
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }

  // object[]
  const jsize len = static_cast<jsize>(thread_status.size());
  jobjectArray jthread_status = env->NewObjectArray(
      len, CABINDB_NAMESPACE::ThreadStatusJni::getJClass(env), nullptr);
  if (jthread_status == nullptr) {
    // an exception occurred
    return nullptr;
  }
  for (jsize i = 0; i < len; ++i) {
    jobject jts =
        CABINDB_NAMESPACE::ThreadStatusJni::construct(env, &(thread_status[i]));
    env->SetObjectArrayElement(jthread_status, i, jts);
    if (env->ExceptionCheck()) {
      // exception occurred
      env->DeleteLocalRef(jthread_status);
      return nullptr;
    }
  }

  return jthread_status;
}

/*
 * Class:     org_cabindb_CabinMemEnv
 * Method:    createMemEnv
 * Signature: (J)J
 */
jlong Java_org_cabindb_CabinMemEnv_createMemEnv(
    JNIEnv*, jclass, jlong jbase_env_handle) {
  auto* base_env = reinterpret_cast<CABINDB_NAMESPACE::Env*>(jbase_env_handle);
  return reinterpret_cast<jlong>(CABINDB_NAMESPACE::NewMemEnv(base_env));
}

/*
 * Class:     org_cabindb_CabinMemEnv
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_cabindb_CabinMemEnv_disposeInternal(
    JNIEnv*, jobject, jlong jhandle) {
  auto* e = reinterpret_cast<CABINDB_NAMESPACE::Env*>(jhandle);
  assert(e != nullptr);
  delete e;
}

/*
 * Class:     org_cabindb_HdfsEnv
 * Method:    createHdfsEnv
 * Signature: (Ljava/lang/String;)J
 */
jlong Java_org_cabindb_HdfsEnv_createHdfsEnv(
    JNIEnv* env, jclass, jstring jfsname) {
  jboolean has_exception = JNI_FALSE;
  auto fsname =
      CABINDB_NAMESPACE::JniUtil::copyStdString(env, jfsname, &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return 0;
  }
  CABINDB_NAMESPACE::Env* hdfs_env;
  CABINDB_NAMESPACE::Status s =
      CABINDB_NAMESPACE::NewHdfsEnv(&hdfs_env, fsname);
  if (!s.ok()) {
    // error occurred
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
    return 0;
  }
  return reinterpret_cast<jlong>(hdfs_env);
}

/*
 * Class:     org_cabindb_HdfsEnv
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_cabindb_HdfsEnv_disposeInternal(
    JNIEnv*, jobject, jlong jhandle) {
  auto* e = reinterpret_cast<CABINDB_NAMESPACE::Env*>(jhandle);
  assert(e != nullptr);
  delete e;
}

/*
 * Class:     org_cabindb_TimedEnv
 * Method:    createTimedEnv
 * Signature: (J)J
 */
jlong Java_org_cabindb_TimedEnv_createTimedEnv(
    JNIEnv*, jclass, jlong jbase_env_handle) {
  auto* base_env = reinterpret_cast<CABINDB_NAMESPACE::Env*>(jbase_env_handle);
  return reinterpret_cast<jlong>(CABINDB_NAMESPACE::NewTimedEnv(base_env));
}

/*
 * Class:     org_cabindb_TimedEnv
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_cabindb_TimedEnv_disposeInternal(
    JNIEnv*, jobject, jlong jhandle) {
  auto* e = reinterpret_cast<CABINDB_NAMESPACE::Env*>(jhandle);
  assert(e != nullptr);
  delete e;
}

