//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <atomic>
#include <map>
#include <string>
#include <vector>

#include "env/composite_env_wrapper.h"
#include "include/cabindb/env.h"
#include "include/cabindb/status.h"

namespace CABINDB_NAMESPACE {

class MockEnv : public CompositeEnvWrapper {
 public:
  explicit MockEnv(Env* base_env);

  // Results of these can be affected by FakeSleepForMicroseconds()
  Status GetCurrentTime(int64_t* unix_time) override;
  uint64_t NowMicros() override;
  uint64_t NowNanos() override;

  Status CorruptBuffer(const std::string& fname);

  // Doesn't really sleep, just affects output of GetCurrentTime(), NowMicros()
  // and NowNanos()
  void FakeSleepForMicroseconds(int64_t micros);

 private:
  std::atomic<int64_t> fake_sleep_micros_;
};

}  // namespace CABINDB_NAMESPACE
