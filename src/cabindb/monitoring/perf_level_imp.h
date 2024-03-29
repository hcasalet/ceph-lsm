//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include "include/cabindb/perf_level.h"
#include "port/port.h"

namespace CABINDB_NAMESPACE {

#ifdef CABINDB_SUPPORT_THREAD_LOCAL
extern __thread PerfLevel perf_level;
#else
extern PerfLevel perf_level;
#endif

}  // namespace CABINDB_NAMESPACE
