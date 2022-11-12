//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// CABINDB_NAMESPACE::CompactionFilterFactory.

#include <jni.h>
#include <memory>

#include "include/org_cabindb_AbstractCompactionFilterFactory.h"
#include "cabinjni/compaction_filter_factory_jnicallback.h"

/*
 * Class:     org_cabindb_AbstractCompactionFilterFactory
 * Method:    createNewCompactionFilterFactory0
 * Signature: ()J
 */
jlong Java_org_cabindb_AbstractCompactionFilterFactory_createNewCompactionFilterFactory0(
    JNIEnv* env, jobject jobj) {
  auto* cff =
      new CABINDB_NAMESPACE::CompactionFilterFactoryJniCallback(env, jobj);
  auto* ptr_sptr_cff = new std::shared_ptr<
      CABINDB_NAMESPACE::CompactionFilterFactoryJniCallback>(cff);
  return reinterpret_cast<jlong>(ptr_sptr_cff);
}

/*
 * Class:     org_cabindb_AbstractCompactionFilterFactory
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_cabindb_AbstractCompactionFilterFactory_disposeInternal(
    JNIEnv*, jobject, jlong jhandle) {
  auto* ptr_sptr_cff = reinterpret_cast<
      std::shared_ptr<CABINDB_NAMESPACE::CompactionFilterFactoryJniCallback>*>(
      jhandle);
  delete ptr_sptr_cff;
}
