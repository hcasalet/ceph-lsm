//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.



#include "lru_cache.h"
#include "include/cabindb/cache.h"
#include "include/cabindb/utilities/options_type.h"
#include "util/string_util.h"

namespace CABINDB_NAMESPACE {
#ifndef CABINDB_LITE
static std::unordered_map<std::string, OptionTypeInfo>
    lru_cache_options_type_info = {
        {"capacity",
         {offsetof(struct LRUCacheOptions, capacity), OptionType::kSizeT,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable}},
        {"num_shard_bits",
         {offsetof(struct LRUCacheOptions, num_shard_bits), OptionType::kInt,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable}},
        {"strict_capacity_limit",
         {offsetof(struct LRUCacheOptions, strict_capacity_limit),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"high_pri_pool_ratio",
         {offsetof(struct LRUCacheOptions, high_pri_pool_ratio),
          OptionType::kDouble, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
};
#endif  // CABINDB_LITE

Status Cache::CreateFromString(const ConfigOptions& config_options,
                               const std::string& value,
                               std::shared_ptr<Cache>* result) {
  Status status;
  std::shared_ptr<Cache> cache;
  if (value.find('=') == std::string::npos) {
    cache = NewLRUCache(ParseSizeT(value));
  } else {
#ifndef CABINDB_LITE
    LRUCacheOptions cache_opts;
    status = OptionTypeInfo::ParseStruct(
        config_options, "", &lru_cache_options_type_info, "", value,
        reinterpret_cast<char*>(&cache_opts));
    if (status.ok()) {
      cache = NewLRUCache(cache_opts);
    }
#else
    (void)config_options;
    status = Status::NotSupported("Cannot load cache in LITE mode ", value);
#endif  //! CABINDB_LITE
  }
  if (status.ok()) {
    result->swap(cache);
  }
  return status;
}
}  // namespace CABINDB_NAMESPACE
