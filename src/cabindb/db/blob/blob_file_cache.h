//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cinttypes>

#include "cache/cache_helpers.h"
#include "include/cabindb/cabindb_namespace.h"
#include "util/mutexlock.h"

namespace CABINDB_NAMESPACE {

class Cache;
struct ImmutableCFOptions;
struct FileOptions;
class HistogramImpl;
class Status;
class BlobFileReader;
class Slice;

class BlobFileCache {
 public:
  BlobFileCache(Cache* cache, const ImmutableCFOptions* immutable_cf_options,
                const FileOptions* file_options, uint32_t column_family_id,
                HistogramImpl* blob_file_read_hist);

  BlobFileCache(const BlobFileCache&) = delete;
  BlobFileCache& operator=(const BlobFileCache&) = delete;

  Status GetBlobFileReader(uint64_t blob_file_number,
                           CacheHandleGuard<BlobFileReader>* blob_file_reader);

 private:
  Cache* cache_;
  // Note: mutex_ below is used to guard against multiple threads racing to open
  // the same file.
  Striped<port::Mutex, Slice> mutex_;
  const ImmutableCFOptions* immutable_cf_options_;
  const FileOptions* file_options_;
  uint32_t column_family_id_;
  HistogramImpl* blob_file_read_hist_;

  static constexpr size_t kNumberOfMutexStripes = 1 << 7;
};

}  // namespace CABINDB_NAMESPACE
