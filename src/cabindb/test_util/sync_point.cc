//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "test_util/sync_point.h"

#include <fcntl.h>

#include "test_util/sync_point_impl.h"

int cabindb_kill_odds = 0;
std::vector<std::string> cabindb_kill_exclude_prefixes;

#ifndef NDEBUG
namespace CABINDB_NAMESPACE {

SyncPoint* SyncPoint::GetInstance() {
  static SyncPoint sync_point;
  return &sync_point;
}

SyncPoint::SyncPoint() : impl_(new Data) {}

SyncPoint:: ~SyncPoint() {
  delete impl_;
}

void SyncPoint::LoadDependency(const std::vector<SyncPointPair>& dependencies) {
  impl_->LoadDependency(dependencies);
}

void SyncPoint::LoadDependencyAndMarkers(
  const std::vector<SyncPointPair>& dependencies,
  const std::vector<SyncPointPair>& markers) {
  impl_->LoadDependencyAndMarkers(dependencies, markers);
}

void SyncPoint::SetCallBack(const std::string& point,
  const std::function<void(void*)>& callback) {
  impl_->SetCallBack(point, callback);
}

void SyncPoint::ClearCallBack(const std::string& point) {
  impl_->ClearCallBack(point);
}

void SyncPoint::ClearAllCallBacks() {
  impl_->ClearAllCallBacks();
}

void SyncPoint::EnableProcessing() {
  impl_->EnableProcessing();
}

void SyncPoint::DisableProcessing() {
  impl_->DisableProcessing();
}

void SyncPoint::ClearTrace() {
  impl_->ClearTrace();
}

void SyncPoint::Process(const std::string& point, void* cb_arg) {
  impl_->Process(point, cb_arg);
}

}  // namespace CABINDB_NAMESPACE
#endif  // NDEBUG

namespace CABINDB_NAMESPACE {
void SetupSyncPointsToMockDirectIO() {
#if !defined(NDEBUG) && !defined(OS_MACOSX) && !defined(OS_WIN) && \
    !defined(OS_SOLARIS) && !defined(OS_AIX) && !defined(OS_OPENBSD)
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "NewWritableFile:O_DIRECT", [&](void* arg) {
        int* val = static_cast<int*>(arg);
        *val &= ~O_DIRECT;
      });
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "NewRandomAccessFile:O_DIRECT", [&](void* arg) {
        int* val = static_cast<int*>(arg);
        *val &= ~O_DIRECT;
      });
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "NewSequentialFile:O_DIRECT", [&](void* arg) {
        int* val = static_cast<int*>(arg);
        *val &= ~O_DIRECT;
      });
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
#endif
}
}  // namespace CABINDB_NAMESPACE
