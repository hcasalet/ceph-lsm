//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Must not be included from any .h files to avoid polluting the namespace
// with macros.

#pragma once

// Helper macros that include information about file name and line number
#define CABIN_LOG_STRINGIFY(x) #x
#define CABIN_LOG_TOSTRING(x) CABIN_LOG_STRINGIFY(x)
#define CABIN_LOG_PREPEND_FILE_LINE(FMT) ("[%s:" CABIN_LOG_TOSTRING(__LINE__) "] " FMT)

inline const char* CabinLogShorterFileName(const char* file)
{
  // 18 is the length of "logging/logging.h".
  // If the name of this file changed, please change this number, too.
  return file + (sizeof(__FILE__) > 18 ? sizeof(__FILE__) - 18 : 0);
}

// Don't inclide file/line info in HEADER level
#define CABIN_LOG_HEADER(LGR, FMT, ...) \
  CABINDB_NAMESPACE::Log(InfoLogLevel::HEADER_LEVEL, LGR, FMT, ##__VA_ARGS__)

#define CABIN_LOG_DEBUG(LGR, FMT, ...)                     \
  CABINDB_NAMESPACE::Log(InfoLogLevel::DEBUG_LEVEL, LGR,   \
                         CABIN_LOG_PREPEND_FILE_LINE(FMT), \
                         CabinLogShorterFileName(__FILE__), ##__VA_ARGS__)

#define CABIN_LOG_INFO(LGR, FMT, ...)                      \
  CABINDB_NAMESPACE::Log(InfoLogLevel::INFO_LEVEL, LGR,    \
                         CABIN_LOG_PREPEND_FILE_LINE(FMT), \
                         CabinLogShorterFileName(__FILE__), ##__VA_ARGS__)

#define CABIN_LOG_WARN(LGR, FMT, ...)                      \
  CABINDB_NAMESPACE::Log(InfoLogLevel::WARN_LEVEL, LGR,    \
                         CABIN_LOG_PREPEND_FILE_LINE(FMT), \
                         CabinLogShorterFileName(__FILE__), ##__VA_ARGS__)

#define CABIN_LOG_ERROR(LGR, FMT, ...)                     \
  CABINDB_NAMESPACE::Log(InfoLogLevel::ERROR_LEVEL, LGR,   \
                         CABIN_LOG_PREPEND_FILE_LINE(FMT), \
                         CabinLogShorterFileName(__FILE__), ##__VA_ARGS__)

#define CABIN_LOG_FATAL(LGR, FMT, ...)                     \
  CABINDB_NAMESPACE::Log(InfoLogLevel::FATAL_LEVEL, LGR,   \
                         CABIN_LOG_PREPEND_FILE_LINE(FMT), \
                         CabinLogShorterFileName(__FILE__), ##__VA_ARGS__)

#define CABIN_LOG_BUFFER(LOG_BUF, FMT, ...)                                 \
  CABINDB_NAMESPACE::LogToBuffer(LOG_BUF, CABIN_LOG_PREPEND_FILE_LINE(FMT), \
                                 CabinLogShorterFileName(__FILE__),         \
                                 ##__VA_ARGS__)

#define CABIN_LOG_BUFFER_MAX_SZ(LOG_BUF, MAX_LOG_SIZE, FMT, ...) \
  CABINDB_NAMESPACE::LogToBuffer(                                \
      LOG_BUF, MAX_LOG_SIZE, CABIN_LOG_PREPEND_FILE_LINE(FMT),   \
      CabinLogShorterFileName(__FILE__), ##__VA_ARGS__)

#define CABIN_LOG_DETAILS(LGR, FMT, ...) \
  ;  // due to overhead by default skip such lines
// CABIN_LOG_DEBUG(LGR, FMT, ##__VA_ARGS__)
