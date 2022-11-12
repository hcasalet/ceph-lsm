//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// CABINDB_NAMESPACE::WalFilter.

#include <jni.h>

#include "include/org_cabindb_AbstractWalFilter.h"
#include "cabinjni/wal_filter_jnicallback.h"

/*
 * Class:     org_cabindb_AbstractWalFilter
 * Method:    createNewWalFilter
 * Signature: ()J
 */
jlong Java_org_cabindb_AbstractWalFilter_createNewWalFilter(
    JNIEnv* env, jobject jobj) {
  auto* wal_filter = new CABINDB_NAMESPACE::WalFilterJniCallback(env, jobj);
  return reinterpret_cast<jlong>(wal_filter);
}
