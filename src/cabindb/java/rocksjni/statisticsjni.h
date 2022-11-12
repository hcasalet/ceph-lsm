// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// CABINDB_NAMESPACE::Statistics

#ifndef JAVA_CABINJNI_STATISTICSJNI_H_
#define JAVA_CABINJNI_STATISTICSJNI_H_

#include <memory>
#include <set>
#include <string>
#include "cabindb/statistics.h"
#include "monitoring/statistics.h"

namespace CABINDB_NAMESPACE {

class StatisticsJni : public StatisticsImpl {
 public:
  StatisticsJni(std::shared_ptr<Statistics> stats);
  StatisticsJni(std::shared_ptr<Statistics> stats,
                const std::set<uint32_t> ignore_histograms);
  virtual bool HistEnabledForType(uint32_t type) const override;

 private:
  const std::set<uint32_t> m_ignore_histograms;
 };

 }  // namespace CABINDB_NAMESPACE

#endif  // JAVA_CABINJNI_STATISTICSJNI_H_
