// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling C++ CABINDB_NAMESPACE::SstFileManager methods
// from Java side.

#include "cabindb/sst_partitioner.h"

#include <jni.h>

#include <memory>

#include "include/org_cabindb_SstPartitionerFixedPrefixFactory.h"
#include "cabindb/sst_file_manager.h"
#include "cabinjni/portal.h"

/*
 * Class:     org_cabindb_SstPartitionerFixedPrefixFactory
 * Method:    newSstPartitionerFixedPrefixFactory0
 * Signature: (J)J
 */
jlong Java_org_cabindb_SstPartitionerFixedPrefixFactory_newSstPartitionerFixedPrefixFactory0(
    JNIEnv*, jclass, jlong prefix_len) {
  auto* ptr = new std::shared_ptr<CABINDB_NAMESPACE::SstPartitionerFactory>(
      CABINDB_NAMESPACE::NewSstPartitionerFixedPrefixFactory(prefix_len));
  return reinterpret_cast<jlong>(ptr);
}

/*
 * Class:     org_cabindb_SstPartitionerFixedPrefixFactory
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_cabindb_SstPartitionerFixedPrefixFactory_disposeInternal(
    JNIEnv*, jobject, jlong jhandle) {
  auto* ptr = reinterpret_cast<
      std::shared_ptr<CABINDB_NAMESPACE::SstPartitionerFactory>*>(jhandle);
  delete ptr;  // delete std::shared_ptr
}
