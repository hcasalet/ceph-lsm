// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef CABINDB_LITE

#pragma once

#include <string>

#include "include/cabindb/compaction_filter.h"
#include "include/cabindb/slice.h"

namespace CABINDB_NAMESPACE {

class RemoveEmptyValueCompactionFilter : public CompactionFilter {
 public:
    const char* Name() const override;
    bool Filter(int level,
        const Slice& key,
        const Slice& existing_value,
        std::string* new_value,
        bool* value_changed) const override;
};
}  // namespace CABINDB_NAMESPACE
#endif  // !CABINDB_LITE
