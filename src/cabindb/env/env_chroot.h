//  Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#if !defined(CABINDB_LITE) && !defined(OS_WIN)

#include <string>

#include "include/cabindb/env.h"

namespace CABINDB_NAMESPACE {

// Returns an Env that translates paths such that the root directory appears to
// be chroot_dir. chroot_dir should refer to an existing directory.
Env* NewChrootEnv(Env* base_env, const std::string& chroot_dir);

}  // namespace CABINDB_NAMESPACE

#endif  // !defined(CABINDB_LITE) && !defined(OS_WIN)
