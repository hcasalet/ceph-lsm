//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <memory>
#include <string>

#include "include/org_cabindb_CassandraValueMergeOperator.h"
#include "cabindb/db.h"
#include "cabindb/memtablerep.h"
#include "cabindb/merge_operator.h"
#include "cabindb/options.h"
#include "cabindb/slice_transform.h"
#include "cabindb/statistics.h"
#include "cabindb/table.h"
#include "cabinjni/portal.h"
#include "utilities/cassandra/merge_operator.h"

/*
 * Class:     org_cabindb_CassandraValueMergeOperator
 * Method:    newSharedCassandraValueMergeOperator
 * Signature: (II)J
 */
jlong Java_org_cabindb_CassandraValueMergeOperator_newSharedCassandraValueMergeOperator(
    JNIEnv* /*env*/, jclass /*jclazz*/, jint gcGracePeriodInSeconds,
    jint operands_limit) {
  auto* op = new std::shared_ptr<CABINDB_NAMESPACE::MergeOperator>(
      new CABINDB_NAMESPACE::cassandra::CassandraValueMergeOperator(
          gcGracePeriodInSeconds, operands_limit));
  return reinterpret_cast<jlong>(op);
}

/*
 * Class:     org_cabindb_CassandraValueMergeOperator
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_cabindb_CassandraValueMergeOperator_disposeInternal(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle) {
  auto* op =
      reinterpret_cast<std::shared_ptr<CABINDB_NAMESPACE::MergeOperator>*>(
          jhandle);
  delete op;
}
