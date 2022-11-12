// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ CABINDB_NAMESPACE::Iterator methods from Java side.

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <algorithm>

#include "include/org_cabindb_CabinIterator.h"
#include "cabindb/iterator.h"
#include "cabinjni/portal.h"

/*
 * Class:     org_cabindb_CabinIterator
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_cabindb_CabinIterator_disposeInternal(JNIEnv* /*env*/,
                                                    jobject /*jobj*/,
                                                    jlong handle) {
  auto* it = reinterpret_cast<CABINDB_NAMESPACE::Iterator*>(handle);
  assert(it != nullptr);
  delete it;
}

/*
 * Class:     org_cabindb_CabinIterator
 * Method:    isValid0
 * Signature: (J)Z
 */
jboolean Java_org_cabindb_CabinIterator_isValid0(JNIEnv* /*env*/,
                                                 jobject /*jobj*/,
                                                 jlong handle) {
  return reinterpret_cast<CABINDB_NAMESPACE::Iterator*>(handle)->Valid();
}

/*
 * Class:     org_cabindb_CabinIterator
 * Method:    seekToFirst0
 * Signature: (J)V
 */
void Java_org_cabindb_CabinIterator_seekToFirst0(JNIEnv* /*env*/,
                                                 jobject /*jobj*/,
                                                 jlong handle) {
  reinterpret_cast<CABINDB_NAMESPACE::Iterator*>(handle)->SeekToFirst();
}

/*
 * Class:     org_cabindb_CabinIterator
 * Method:    seekToLast0
 * Signature: (J)V
 */
void Java_org_cabindb_CabinIterator_seekToLast0(JNIEnv* /*env*/,
                                                jobject /*jobj*/,
                                                jlong handle) {
  reinterpret_cast<CABINDB_NAMESPACE::Iterator*>(handle)->SeekToLast();
}

/*
 * Class:     org_cabindb_CabinIterator
 * Method:    next0
 * Signature: (J)V
 */
void Java_org_cabindb_CabinIterator_next0(JNIEnv* /*env*/, jobject /*jobj*/,
                                          jlong handle) {
  reinterpret_cast<CABINDB_NAMESPACE::Iterator*>(handle)->Next();
}

/*
 * Class:     org_cabindb_CabinIterator
 * Method:    prev0
 * Signature: (J)V
 */
void Java_org_cabindb_CabinIterator_prev0(JNIEnv* /*env*/, jobject /*jobj*/,
                                          jlong handle) {
  reinterpret_cast<CABINDB_NAMESPACE::Iterator*>(handle)->Prev();
}

/*
 * Class:     org_cabindb_CabinIterator
 * Method:    refresh0
 * Signature: (J)V
 */
void Java_org_cabindb_CabinIterator_refresh0(JNIEnv* env, jobject /*jobj*/,
                                            jlong handle) {
  auto* it = reinterpret_cast<CABINDB_NAMESPACE::Iterator*>(handle);
  CABINDB_NAMESPACE::Status s = it->Refresh();

  if (s.ok()) {
    return;
  }

  CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_cabindb_CabinIterator
 * Method:    seek0
 * Signature: (J[BI)V
 */
void Java_org_cabindb_CabinIterator_seek0(JNIEnv* env, jobject /*jobj*/,
                                          jlong handle, jbyteArray jtarget,
                                          jint jtarget_len) {
  jbyte* target = env->GetByteArrayElements(jtarget, nullptr);
  if (target == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  CABINDB_NAMESPACE::Slice target_slice(reinterpret_cast<char*>(target),
                                        jtarget_len);

  auto* it = reinterpret_cast<CABINDB_NAMESPACE::Iterator*>(handle);
  it->Seek(target_slice);

  env->ReleaseByteArrayElements(jtarget, target, JNI_ABORT);
}

/*
 * Class:     org_cabindb_CabinIterator
 * Method:    seekDirect0
 * Signature: (JLjava/nio/ByteBuffer;II)V
 */
void Java_org_cabindb_CabinIterator_seekDirect0(JNIEnv* env, jobject /*jobj*/,
                                                jlong handle, jobject jtarget,
                                                jint jtarget_off,
                                                jint jtarget_len) {
  auto* it = reinterpret_cast<CABINDB_NAMESPACE::Iterator*>(handle);
  auto seek = [&it](CABINDB_NAMESPACE::Slice& target_slice) {
    it->Seek(target_slice);
  };
  CABINDB_NAMESPACE::JniUtil::k_op_direct(seek, env, jtarget, jtarget_off,
                                          jtarget_len);
}

/*
 * Class:     org_cabindb_CabinIterator
 * Method:    seekForPrevDirect0
 * Signature: (JLjava/nio/ByteBuffer;II)V
 */
void Java_org_cabindb_CabinIterator_seekForPrevDirect0(
    JNIEnv* env, jobject /*jobj*/, jlong handle, jobject jtarget,
    jint jtarget_off, jint jtarget_len) {
  auto* it = reinterpret_cast<CABINDB_NAMESPACE::Iterator*>(handle);
  auto seekPrev = [&it](CABINDB_NAMESPACE::Slice& target_slice) {
    it->SeekForPrev(target_slice);
  };
  CABINDB_NAMESPACE::JniUtil::k_op_direct(seekPrev, env, jtarget, jtarget_off,
                                          jtarget_len);
}

/*
 * Class:     org_cabindb_CabinIterator
 * Method:    seekForPrev0
 * Signature: (J[BI)V
 */
void Java_org_cabindb_CabinIterator_seekForPrev0(JNIEnv* env, jobject /*jobj*/,
                                                 jlong handle,
                                                 jbyteArray jtarget,
                                                 jint jtarget_len) {
  jbyte* target = env->GetByteArrayElements(jtarget, nullptr);
  if (target == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  CABINDB_NAMESPACE::Slice target_slice(reinterpret_cast<char*>(target),
                                        jtarget_len);

  auto* it = reinterpret_cast<CABINDB_NAMESPACE::Iterator*>(handle);
  it->SeekForPrev(target_slice);

  env->ReleaseByteArrayElements(jtarget, target, JNI_ABORT);
}

/*
 * Class:     org_cabindb_CabinIterator
 * Method:    status0
 * Signature: (J)V
 */
void Java_org_cabindb_CabinIterator_status0(JNIEnv* env, jobject /*jobj*/,
                                            jlong handle) {
  auto* it = reinterpret_cast<CABINDB_NAMESPACE::Iterator*>(handle);
  CABINDB_NAMESPACE::Status s = it->status();

  if (s.ok()) {
    return;
  }

  CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_cabindb_CabinIterator
 * Method:    key0
 * Signature: (J)[B
 */
jbyteArray Java_org_cabindb_CabinIterator_key0(JNIEnv* env, jobject /*jobj*/,
                                               jlong handle) {
  auto* it = reinterpret_cast<CABINDB_NAMESPACE::Iterator*>(handle);
  CABINDB_NAMESPACE::Slice key_slice = it->key();

  jbyteArray jkey = env->NewByteArray(static_cast<jsize>(key_slice.size()));
  if (jkey == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }
  env->SetByteArrayRegion(
      jkey, 0, static_cast<jsize>(key_slice.size()),
      const_cast<jbyte*>(reinterpret_cast<const jbyte*>(key_slice.data())));
  return jkey;
}

/*
 * Class:     org_cabindb_CabinIterator
 * Method:    keyDirect0
 * Signature: (JLjava/nio/ByteBuffer;II)I
 */
jint Java_org_cabindb_CabinIterator_keyDirect0(JNIEnv* env, jobject /*jobj*/,
                                               jlong handle, jobject jtarget,
                                               jint jtarget_off,
                                               jint jtarget_len) {
  auto* it = reinterpret_cast<CABINDB_NAMESPACE::Iterator*>(handle);
  CABINDB_NAMESPACE::Slice key_slice = it->key();
  return CABINDB_NAMESPACE::JniUtil::copyToDirect(env, key_slice, jtarget,
                                                  jtarget_off, jtarget_len);
}

/*
 * Class:     org_cabindb_CabinIterator
 * Method:    value0
 * Signature: (J)[B
 */
jbyteArray Java_org_cabindb_CabinIterator_value0(JNIEnv* env, jobject /*jobj*/,
                                                 jlong handle) {
  auto* it = reinterpret_cast<CABINDB_NAMESPACE::Iterator*>(handle);
  CABINDB_NAMESPACE::Slice value_slice = it->value();

  jbyteArray jkeyValue =
      env->NewByteArray(static_cast<jsize>(value_slice.size()));
  if (jkeyValue == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }
  env->SetByteArrayRegion(
      jkeyValue, 0, static_cast<jsize>(value_slice.size()),
      const_cast<jbyte*>(reinterpret_cast<const jbyte*>(value_slice.data())));
  return jkeyValue;
}

/*
 * Class:     org_cabindb_CabinIterator
 * Method:    valueDirect0
 * Signature: (JLjava/nio/ByteBuffer;II)I
 */
jint Java_org_cabindb_CabinIterator_valueDirect0(JNIEnv* env, jobject /*jobj*/,
                                                 jlong handle, jobject jtarget,
                                                 jint jtarget_off,
                                                 jint jtarget_len) {
  auto* it = reinterpret_cast<CABINDB_NAMESPACE::Iterator*>(handle);
  CABINDB_NAMESPACE::Slice value_slice = it->value();
  return CABINDB_NAMESPACE::JniUtil::copyToDirect(env, value_slice, jtarget,
                                                  jtarget_off, jtarget_len);
}
