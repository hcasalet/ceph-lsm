// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2014, Vlad Balan (vlad.gm@gmail.com).  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++
// for CABINDB_NAMESPACE::MergeOperator.

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <memory>
#include <string>

#include "include/org_cabindb_StringAppendOperator.h"
#include "include/org_cabindb_UInt64AddOperator.h"
#include "cabindb/db.h"
#include "cabindb/memtablerep.h"
#include "cabindb/merge_operator.h"
#include "cabindb/options.h"
#include "cabindb/slice_transform.h"
#include "cabindb/statistics.h"
#include "cabindb/table.h"
#include "cabinjni/portal.h"
#include "utilities/merge_operators.h"

/*
 * Class:     org_cabindb_StringAppendOperator
 * Method:    newSharedStringAppendOperator
 * Signature: (C)J
 */
jlong Java_org_cabindb_StringAppendOperator_newSharedStringAppendOperator(
    JNIEnv* /*env*/, jclass /*jclazz*/, jchar jdelim) {
  auto* sptr_string_append_op =
      new std::shared_ptr<CABINDB_NAMESPACE::MergeOperator>(
          CABINDB_NAMESPACE::MergeOperators::CreateStringAppendOperator(
              (char)jdelim));
  return reinterpret_cast<jlong>(sptr_string_append_op);
}

/*
 * Class:     org_cabindb_StringAppendOperator
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_cabindb_StringAppendOperator_disposeInternal(JNIEnv* /*env*/,
                                                           jobject /*jobj*/,
                                                           jlong jhandle) {
  auto* sptr_string_append_op =
      reinterpret_cast<std::shared_ptr<CABINDB_NAMESPACE::MergeOperator>*>(
          jhandle);
  delete sptr_string_append_op;  // delete std::shared_ptr
}

/*
 * Class:     org_cabindb_UInt64AddOperator
 * Method:    newSharedUInt64AddOperator
 * Signature: ()J
 */
jlong Java_org_cabindb_UInt64AddOperator_newSharedUInt64AddOperator(
    JNIEnv* /*env*/, jclass /*jclazz*/) {
  auto* sptr_uint64_add_op =
      new std::shared_ptr<CABINDB_NAMESPACE::MergeOperator>(
          CABINDB_NAMESPACE::MergeOperators::CreateUInt64AddOperator());
  return reinterpret_cast<jlong>(sptr_uint64_add_op);
}

/*
 * Class:     org_cabindb_UInt64AddOperator
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_cabindb_UInt64AddOperator_disposeInternal(JNIEnv* /*env*/,
                                                        jobject /*jobj*/,
                                                        jlong jhandle) {
  auto* sptr_uint64_add_op =
      reinterpret_cast<std::shared_ptr<CABINDB_NAMESPACE::MergeOperator>*>(
          jhandle);
  delete sptr_uint64_add_op;  // delete std::shared_ptr
}
