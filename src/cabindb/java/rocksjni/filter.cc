// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// CABINDB_NAMESPACE::FilterPolicy.

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>

#include "include/org_cabindb_BloomFilter.h"
#include "include/org_cabindb_Filter.h"
#include "cabindb/filter_policy.h"
#include "cabinjni/portal.h"

/*
 * Class:     org_cabindb_BloomFilter
 * Method:    createBloomFilter
 * Signature: (DZ)J
 */
jlong Java_org_cabindb_BloomFilter_createNewBloomFilter(
    JNIEnv* /*env*/, jclass /*jcls*/, jdouble bits_per_key,
    jboolean use_block_base_builder) {
  auto* sptr_filter =
      new std::shared_ptr<const CABINDB_NAMESPACE::FilterPolicy>(
          CABINDB_NAMESPACE::NewBloomFilterPolicy(bits_per_key,
                                                  use_block_base_builder));
  return reinterpret_cast<jlong>(sptr_filter);
}

/*
 * Class:     org_cabindb_Filter
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_cabindb_Filter_disposeInternal(JNIEnv* /*env*/, jobject /*jobj*/,
                                             jlong jhandle) {
  auto* handle =
      reinterpret_cast<std::shared_ptr<const CABINDB_NAMESPACE::FilterPolicy>*>(
          jhandle);
  delete handle;  // delete std::shared_ptr
}
