// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling C++ CABINDB_NAMESPACE::SstFileWriter methods
// from Java side.

#include <jni.h>
#include <string>

#include "include/org_cabindb_SstFileWriter.h"
#include "cabindb/comparator.h"
#include "cabindb/env.h"
#include "cabindb/options.h"
#include "cabindb/sst_file_writer.h"
#include "cabinjni/portal.h"

/*
 * Class:     org_cabindb_SstFileWriter
 * Method:    newSstFileWriter
 * Signature: (JJJB)J
 */
jlong Java_org_cabindb_SstFileWriter_newSstFileWriter__JJJB(
    JNIEnv * /*env*/, jclass /*jcls*/, jlong jenvoptions, jlong joptions,
    jlong jcomparator_handle, jbyte jcomparator_type) {
  CABINDB_NAMESPACE::Comparator *comparator = nullptr;
  switch (jcomparator_type) {
    // JAVA_COMPARATOR
    case 0x0:
      comparator = reinterpret_cast<CABINDB_NAMESPACE::ComparatorJniCallback *>(
          jcomparator_handle);
      break;

    // JAVA_NATIVE_COMPARATOR_WRAPPER
    case 0x1:
      comparator =
          reinterpret_cast<CABINDB_NAMESPACE::Comparator *>(jcomparator_handle);
      break;
  }
  auto *env_options =
      reinterpret_cast<const CABINDB_NAMESPACE::EnvOptions *>(jenvoptions);
  auto *options =
      reinterpret_cast<const CABINDB_NAMESPACE::Options *>(joptions);
  CABINDB_NAMESPACE::SstFileWriter *sst_file_writer =
      new CABINDB_NAMESPACE::SstFileWriter(*env_options, *options, comparator);
  return reinterpret_cast<jlong>(sst_file_writer);
}

/*
 * Class:     org_cabindb_SstFileWriter
 * Method:    newSstFileWriter
 * Signature: (JJ)J
 */
jlong Java_org_cabindb_SstFileWriter_newSstFileWriter__JJ(JNIEnv * /*env*/,
                                                          jclass /*jcls*/,
                                                          jlong jenvoptions,
                                                          jlong joptions) {
  auto *env_options =
      reinterpret_cast<const CABINDB_NAMESPACE::EnvOptions *>(jenvoptions);
  auto *options =
      reinterpret_cast<const CABINDB_NAMESPACE::Options *>(joptions);
  CABINDB_NAMESPACE::SstFileWriter *sst_file_writer =
      new CABINDB_NAMESPACE::SstFileWriter(*env_options, *options);
  return reinterpret_cast<jlong>(sst_file_writer);
}

/*
 * Class:     org_cabindb_SstFileWriter
 * Method:    open
 * Signature: (JLjava/lang/String;)V
 */
void Java_org_cabindb_SstFileWriter_open(JNIEnv *env, jobject /*jobj*/,
                                         jlong jhandle, jstring jfile_path) {
  const char *file_path = env->GetStringUTFChars(jfile_path, nullptr);
  if (file_path == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }
  CABINDB_NAMESPACE::Status s =
      reinterpret_cast<CABINDB_NAMESPACE::SstFileWriter *>(jhandle)->Open(
          file_path);
  env->ReleaseStringUTFChars(jfile_path, file_path);

  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_cabindb_SstFileWriter
 * Method:    put
 * Signature: (JJJ)V
 */
void Java_org_cabindb_SstFileWriter_put__JJJ(JNIEnv *env, jobject /*jobj*/,
                                             jlong jhandle, jlong jkey_handle,
                                             jlong jvalue_handle) {
  auto *key_slice = reinterpret_cast<CABINDB_NAMESPACE::Slice *>(jkey_handle);
  auto *value_slice =
      reinterpret_cast<CABINDB_NAMESPACE::Slice *>(jvalue_handle);
  CABINDB_NAMESPACE::Status s =
      reinterpret_cast<CABINDB_NAMESPACE::SstFileWriter *>(jhandle)->Put(
          *key_slice, *value_slice);
  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_cabindb_SstFileWriter
 * Method:    put
 * Signature: (JJJ)V
 */
void Java_org_cabindb_SstFileWriter_put__J_3B_3B(JNIEnv *env, jobject /*jobj*/,
                                                 jlong jhandle, jbyteArray jkey,
                                                 jbyteArray jval) {
  jbyte *key = env->GetByteArrayElements(jkey, nullptr);
  if (key == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }
  CABINDB_NAMESPACE::Slice key_slice(reinterpret_cast<char *>(key),
                                     env->GetArrayLength(jkey));

  jbyte *value = env->GetByteArrayElements(jval, nullptr);
  if (value == nullptr) {
    // exception thrown: OutOfMemoryError
    env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
    return;
  }
  CABINDB_NAMESPACE::Slice value_slice(reinterpret_cast<char *>(value),
                                       env->GetArrayLength(jval));

  CABINDB_NAMESPACE::Status s =
      reinterpret_cast<CABINDB_NAMESPACE::SstFileWriter *>(jhandle)->Put(
          key_slice, value_slice);

  env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
  env->ReleaseByteArrayElements(jval, value, JNI_ABORT);

  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_cabindb_SstFileWriter
 * Method:    putDirect
 * Signature: (JLjava/nio/ByteBuffer;IILjava/nio/ByteBuffer;II)V
 */
void Java_org_cabindb_SstFileWriter_putDirect(JNIEnv *env, jobject /*jdb*/,
                                              jlong jdb_handle, jobject jkey,
                                              jint jkey_off, jint jkey_len,
                                              jobject jval, jint jval_off,
                                              jint jval_len) {
  auto *writer =
      reinterpret_cast<CABINDB_NAMESPACE::SstFileWriter *>(jdb_handle);
  auto put = [&env, &writer](CABINDB_NAMESPACE::Slice &key,
                             CABINDB_NAMESPACE::Slice &value) {
    CABINDB_NAMESPACE::Status s = writer->Put(key, value);
    if (s.ok()) {
      return;
    }
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  };
  CABINDB_NAMESPACE::JniUtil::kv_op_direct(put, env, jkey, jkey_off, jkey_len,
                                           jval, jval_off, jval_len);
}

/*
 * Class:     org_cabindb_SstFileWriter
 * Method:    fileSize
 * Signature: (J)J
 */
jlong Java_org_cabindb_SstFileWriter_fileSize(JNIEnv * /*env*/, jobject /*jdb*/,
                                              jlong jdb_handle) {
  auto *writer =
      reinterpret_cast<CABINDB_NAMESPACE::SstFileWriter *>(jdb_handle);
  return static_cast<jlong>(writer->FileSize());
}

/*
 * Class:     org_cabindb_SstFileWriter
 * Method:    merge
 * Signature: (JJJ)V
 */
void Java_org_cabindb_SstFileWriter_merge__JJJ(JNIEnv *env, jobject /*jobj*/,
                                               jlong jhandle, jlong jkey_handle,
                                               jlong jvalue_handle) {
  auto *key_slice = reinterpret_cast<CABINDB_NAMESPACE::Slice *>(jkey_handle);
  auto *value_slice =
      reinterpret_cast<CABINDB_NAMESPACE::Slice *>(jvalue_handle);
  CABINDB_NAMESPACE::Status s =
      reinterpret_cast<CABINDB_NAMESPACE::SstFileWriter *>(jhandle)->Merge(
          *key_slice, *value_slice);
  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_cabindb_SstFileWriter
 * Method:    merge
 * Signature: (J[B[B)V
 */
void Java_org_cabindb_SstFileWriter_merge__J_3B_3B(JNIEnv *env,
                                                   jobject /*jobj*/,
                                                   jlong jhandle,
                                                   jbyteArray jkey,
                                                   jbyteArray jval) {
  jbyte *key = env->GetByteArrayElements(jkey, nullptr);
  if (key == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }
  CABINDB_NAMESPACE::Slice key_slice(reinterpret_cast<char *>(key),
                                     env->GetArrayLength(jkey));

  jbyte *value = env->GetByteArrayElements(jval, nullptr);
  if (value == nullptr) {
    // exception thrown: OutOfMemoryError
    env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
    return;
  }
  CABINDB_NAMESPACE::Slice value_slice(reinterpret_cast<char *>(value),
                                       env->GetArrayLength(jval));

  CABINDB_NAMESPACE::Status s =
      reinterpret_cast<CABINDB_NAMESPACE::SstFileWriter *>(jhandle)->Merge(
          key_slice, value_slice);

  env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
  env->ReleaseByteArrayElements(jval, value, JNI_ABORT);

  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_cabindb_SstFileWriter
 * Method:    delete
 * Signature: (JJJ)V
 */
void Java_org_cabindb_SstFileWriter_delete__J_3B(JNIEnv *env, jobject /*jobj*/,
                                                 jlong jhandle,
                                                 jbyteArray jkey) {
  jbyte *key = env->GetByteArrayElements(jkey, nullptr);
  if (key == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }
  CABINDB_NAMESPACE::Slice key_slice(reinterpret_cast<char *>(key),
                                     env->GetArrayLength(jkey));

  CABINDB_NAMESPACE::Status s =
      reinterpret_cast<CABINDB_NAMESPACE::SstFileWriter *>(jhandle)->Delete(
          key_slice);

  env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);

  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_cabindb_SstFileWriter
 * Method:    delete
 * Signature: (JJJ)V
 */
void Java_org_cabindb_SstFileWriter_delete__JJ(JNIEnv *env, jobject /*jobj*/,
                                               jlong jhandle,
                                               jlong jkey_handle) {
  auto *key_slice = reinterpret_cast<CABINDB_NAMESPACE::Slice *>(jkey_handle);
  CABINDB_NAMESPACE::Status s =
      reinterpret_cast<CABINDB_NAMESPACE::SstFileWriter *>(jhandle)->Delete(
          *key_slice);
  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_cabindb_SstFileWriter
 * Method:    finish
 * Signature: (J)V
 */
void Java_org_cabindb_SstFileWriter_finish(JNIEnv *env, jobject /*jobj*/,
                                           jlong jhandle) {
  CABINDB_NAMESPACE::Status s =
      reinterpret_cast<CABINDB_NAMESPACE::SstFileWriter *>(jhandle)->Finish();
  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_cabindb_SstFileWriter
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_cabindb_SstFileWriter_disposeInternal(JNIEnv * /*env*/,
                                                    jobject /*jobj*/,
                                                    jlong jhandle) {
  delete reinterpret_cast<CABINDB_NAMESPACE::SstFileWriter *>(jhandle);
}
