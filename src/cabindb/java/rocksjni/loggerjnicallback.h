// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// CABINDB_NAMESPACE::Logger

#ifndef JAVA_CABINJNI_LOGGERJNICALLBACK_H_
#define JAVA_CABINJNI_LOGGERJNICALLBACK_H_

#include <jni.h>
#include <memory>
#include <string>
#include "cabinjni/jnicallback.h"
#include "port/port.h"
#include "cabindb/env.h"

namespace CABINDB_NAMESPACE {

class LoggerJniCallback : public JniCallback, public Logger {
 public:
  LoggerJniCallback(JNIEnv* env, jobject jLogger);
  ~LoggerJniCallback();

  using Logger::SetInfoLogLevel;
  using Logger::GetInfoLogLevel;
  // Write an entry to the log file with the specified format.
  virtual void Logv(const char* format, va_list ap);
  // Write an entry to the log file with the specified log level
  // and format.  Any log with level under the internal log level
  // of *this (see @SetInfoLogLevel and @GetInfoLogLevel) will not be
  // printed.
  virtual void Logv(const InfoLogLevel log_level, const char* format,
                    va_list ap);

 private:
  jmethodID m_jLogMethodId;
  jobject m_jdebug_level;
  jobject m_jinfo_level;
  jobject m_jwarn_level;
  jobject m_jerror_level;
  jobject m_jfatal_level;
  jobject m_jheader_level;
  std::unique_ptr<char[]> format_str(const char* format, va_list ap) const;
  };
  }  // namespace CABINDB_NAMESPACE

#endif  // JAVA_CABINJNI_LOGGERJNICALLBACK_H_
