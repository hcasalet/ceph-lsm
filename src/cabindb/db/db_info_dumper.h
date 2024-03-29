//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <string>

#include "options/db_options.h"

namespace CABINDB_NAMESPACE {
void DumpDBFileSummary(const ImmutableDBOptions& options,
                       const std::string& dbname,
                       const std::string& session_id = "");
}  // namespace CABINDB_NAMESPACE
