// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling C++ CABINDB_NAMESPACE::ConfigOptions methods
// from Java side.

#include <jni.h>

#include "include/org_cabindb_ConfigOptions.h"
#include "cabindb/convenience.h"
#include "cabinjni/portal.h"

/*
 * Class:     org_cabindb_ConfigOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_cabindb_ConfigOptions_disposeInternal(JNIEnv *, jobject,
                                                    jlong jhandle) {
  auto *co = reinterpret_cast<CABINDB_NAMESPACE::ConfigOptions *>(jhandle);
  assert(co != nullptr);
  delete co;
}

/*
 * Class:     org_cabindb_ConfigOptions
 * Method:    newConfigOptions
 * Signature: ()J
 */
jlong Java_org_cabindb_ConfigOptions_newConfigOptions(JNIEnv *, jclass) {
  auto *cfg_opt = new CABINDB_NAMESPACE::ConfigOptions();
  return reinterpret_cast<jlong>(cfg_opt);
}

/*
 * Class:     org_cabindb_ConfigOptions
 * Method:    setDelimiter
 * Signature: (JLjava/lang/String;)V
 */
void Java_org_cabindb_ConfigOptions_setDelimiter(JNIEnv *env, jclass,
                                                 jlong handle, jstring s) {
  auto *cfg_opt = reinterpret_cast<CABINDB_NAMESPACE::ConfigOptions *>(handle);
  const char *delim = env->GetStringUTFChars(s, nullptr);
  if (delim == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }
  cfg_opt->delimiter = delim;
  env->ReleaseStringUTFChars(s, delim);
}

/*
 * Class:     org_cabindb_ConfigOptions
 * Method:    setIgnoreUnknownOptions
 * Signature: (JZ)V
 */
void Java_org_cabindb_ConfigOptions_setIgnoreUnknownOptions(JNIEnv *, jclass,
                                                            jlong handle,
                                                            jboolean b) {
  auto *cfg_opt = reinterpret_cast<CABINDB_NAMESPACE::ConfigOptions *>(handle);
  cfg_opt->ignore_unknown_options = static_cast<bool>(b);
}

/*
 * Class:     org_cabindb_ConfigOptions
 * Method:    setInputStringsEscaped
 * Signature: (JZ)V
 */
void Java_org_cabindb_ConfigOptions_setInputStringsEscaped(JNIEnv *, jclass,
                                                           jlong handle,
                                                           jboolean b) {
  auto *cfg_opt = reinterpret_cast<CABINDB_NAMESPACE::ConfigOptions *>(handle);
  cfg_opt->input_strings_escaped = static_cast<bool>(b);
}

/*
 * Class:     org_cabindb_ConfigOptions
 * Method:    setSanityLevel
 * Signature: (JI)V
 */
void Java_org_cabindb_ConfigOptions_setSanityLevel(JNIEnv *, jclass,
                                                   jlong handle, jbyte level) {
  auto *cfg_opt = reinterpret_cast<CABINDB_NAMESPACE::ConfigOptions *>(handle);
  cfg_opt->sanity_level = CABINDB_NAMESPACE::SanityLevelJni::toCppSanityLevel(level);
}
