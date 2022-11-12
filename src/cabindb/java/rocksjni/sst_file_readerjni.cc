// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling C++ CABINDB_NAMESPACE::SstFileReader methods
// from Java side.

#include <jni.h>
#include <string>

#include "include/org_cabindb_SstFileReader.h"
#include "cabindb/comparator.h"
#include "cabindb/env.h"
#include "cabindb/options.h"
#include "cabindb/sst_file_reader.h"
#include "cabinjni/portal.h"

/*
 * Class:     org_cabindb_SstFileReader
 * Method:    newSstFileReader
 * Signature: (J)J
 */
jlong Java_org_cabindb_SstFileReader_newSstFileReader(JNIEnv * /*env*/,
                                                      jclass /*jcls*/,
                                                      jlong joptions) {
  auto *options =
      reinterpret_cast<const CABINDB_NAMESPACE::Options *>(joptions);
  CABINDB_NAMESPACE::SstFileReader *sst_file_reader =
      new CABINDB_NAMESPACE::SstFileReader(*options);
  return reinterpret_cast<jlong>(sst_file_reader);
}

/*
 * Class:     org_cabindb_SstFileReader
 * Method:    open
 * Signature: (JLjava/lang/String;)V
 */
void Java_org_cabindb_SstFileReader_open(JNIEnv *env, jobject /*jobj*/,
                                         jlong jhandle, jstring jfile_path) {
  const char *file_path = env->GetStringUTFChars(jfile_path, nullptr);
  if (file_path == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }
  CABINDB_NAMESPACE::Status s =
      reinterpret_cast<CABINDB_NAMESPACE::SstFileReader *>(jhandle)->Open(
          file_path);
  env->ReleaseStringUTFChars(jfile_path, file_path);

  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_cabindb_SstFileReader
 * Method:    newIterator
 * Signature: (JJ)J
 */
jlong Java_org_cabindb_SstFileReader_newIterator(JNIEnv * /*env*/,
                                                 jobject /*jobj*/,
                                                 jlong jhandle,
                                                 jlong jread_options_handle) {
  auto *sst_file_reader =
      reinterpret_cast<CABINDB_NAMESPACE::SstFileReader *>(jhandle);
  auto *read_options =
      reinterpret_cast<CABINDB_NAMESPACE::ReadOptions *>(jread_options_handle);
  return reinterpret_cast<jlong>(sst_file_reader->NewIterator(*read_options));
}

/*
 * Class:     org_cabindb_SstFileReader
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_cabindb_SstFileReader_disposeInternal(JNIEnv * /*env*/,
                                                    jobject /*jobj*/,
                                                    jlong jhandle) {
  delete reinterpret_cast<CABINDB_NAMESPACE::SstFileReader *>(jhandle);
}

/*
 * Class:     org_cabindb_SstFileReader
 * Method:    verifyChecksum
 * Signature: (J)V
 */
void Java_org_cabindb_SstFileReader_verifyChecksum(JNIEnv *env,
                                                   jobject /*jobj*/,
                                                   jlong jhandle) {
  auto *sst_file_reader =
      reinterpret_cast<CABINDB_NAMESPACE::SstFileReader *>(jhandle);
  auto s = sst_file_reader->VerifyChecksum();
  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_cabindb_SstFileReader
 * Method:    getTableProperties
 * Signature: (J)J
 */
jobject Java_org_cabindb_SstFileReader_getTableProperties(JNIEnv *env,
                                                          jobject /*jobj*/,
                                                          jlong jhandle) {
  auto *sst_file_reader =
      reinterpret_cast<CABINDB_NAMESPACE::SstFileReader *>(jhandle);
  std::shared_ptr<const CABINDB_NAMESPACE::TableProperties> tp =
      sst_file_reader->GetTableProperties();
  jobject jtable_properties =
      CABINDB_NAMESPACE::TablePropertiesJni::fromCppTableProperties(
          env, *(tp.get()));
  return jtable_properties;
}
