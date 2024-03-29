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
#include <string>

#include "env/file_system_tracer.h"
#include "port/port.h"
#include "include/cabindb/env.h"
#include "include/cabindb/file_system.h"

namespace CABINDB_NAMESPACE {

// SequentialFileReader is a wrapper on top of Env::SequentialFile. It handles
// Buffered (i.e when page cache is enabled) and Direct (with O_DIRECT / page
// cache disabled) reads appropriately, and also updates the IO stats.
class SequentialFileReader {
 private:
  std::string file_name_;
  FSSequentialFilePtr file_;
  std::atomic<size_t> offset_{0};  // read offset

 public:
  explicit SequentialFileReader(
      std::unique_ptr<FSSequentialFile>&& _file, const std::string& _file_name,
      const std::shared_ptr<IOTracer>& io_tracer = nullptr)
      : file_name_(_file_name), file_(std::move(_file), io_tracer) {}

  explicit SequentialFileReader(
      std::unique_ptr<FSSequentialFile>&& _file, const std::string& _file_name,
      size_t _readahead_size,
      const std::shared_ptr<IOTracer>& io_tracer = nullptr)
      : file_name_(_file_name),
        file_(NewReadaheadSequentialFile(std::move(_file), _readahead_size),
              io_tracer) {}

  SequentialFileReader(const SequentialFileReader&) = delete;
  SequentialFileReader& operator=(const SequentialFileReader&) = delete;

  Status Read(size_t n, Slice* result, char* scratch);

  Status Skip(uint64_t n);

  FSSequentialFile* file() { return file_.get(); }

  std::string file_name() { return file_name_; }

  bool use_direct_io() const { return file_->use_direct_io(); }

 private:
  // NewReadaheadSequentialFile provides a wrapper over SequentialFile to
  // always prefetch additional data with every read.
  static std::unique_ptr<FSSequentialFile> NewReadaheadSequentialFile(
      std::unique_ptr<FSSequentialFile>&& file, size_t readahead_size);
};
}  // namespace CABINDB_NAMESPACE
