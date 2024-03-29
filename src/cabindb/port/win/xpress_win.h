//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <string>

#include "cabindb/cabindb_namespace.h"

namespace CABINDB_NAMESPACE {
namespace port {
namespace xpress {

bool Compress(const char* input, size_t length, std::string* output);

char* Decompress(const char* input_data, size_t input_length,
                 size_t* uncompressed_size);
}
}
}  // namespace CABINDB_NAMESPACE
