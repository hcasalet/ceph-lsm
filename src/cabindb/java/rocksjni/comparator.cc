// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// CABINDB_NAMESPACE::Comparator.

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <functional>
#include <string>

#include "include/org_cabindb_AbstractComparator.h"
#include "include/org_cabindb_NativeComparatorWrapper.h"
#include "cabinjni/comparatorjnicallback.h"
#include "cabinjni/portal.h"

/*
 * Class:     org_cabindb_AbstractComparator
 * Method:    createNewComparator
 * Signature: (J)J
 */
jlong Java_org_cabindb_AbstractComparator_createNewComparator(
    JNIEnv* env, jobject jcomparator, jlong copt_handle) {
  auto* copt =
      reinterpret_cast<CABINDB_NAMESPACE::ComparatorJniCallbackOptions*>(
          copt_handle);
  auto* c =
      new CABINDB_NAMESPACE::ComparatorJniCallback(env, jcomparator, copt);
  return reinterpret_cast<jlong>(c);
}

/*
 * Class:     org_cabindb_AbstractComparator
 * Method:    usingDirectBuffers
 * Signature: (J)Z
 */
jboolean Java_org_cabindb_AbstractComparator_usingDirectBuffers(
    JNIEnv*, jobject, jlong jhandle) {
  auto* c =
      reinterpret_cast<CABINDB_NAMESPACE::ComparatorJniCallback*>(jhandle);
  return static_cast<jboolean>(c->m_options->direct_buffer);
}

/*
 * Class:     org_cabindb_NativeComparatorWrapper
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_cabindb_NativeComparatorWrapper_disposeInternal(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jcomparator_handle) {
  auto* comparator =
      reinterpret_cast<CABINDB_NAMESPACE::Comparator*>(jcomparator_handle);
  delete comparator;
}
