//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#ifndef CABINDB_LITE

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "include/cabindb/io_status.h"
#include "include/cabindb/perf_context.h"
#include "include/cabindb/sst_file_manager.h"
#if !defined(CABINDB_LITE)
#include "test_util/sync_point.h"
#endif
#include "util/random.h"
#include "utilities/fault_injection_env.h"
#include "utilities/fault_injection_fs.h"

namespace CABINDB_NAMESPACE {

class DBErrorHandlingFSTest : public DBTestBase {
 public:
  DBErrorHandlingFSTest()
      : DBTestBase("/db_error_handling_fs_test", /*env_do_fsync=*/true) {
    fault_fs_.reset(new FaultInjectionTestFS(env_->GetFileSystem()));
    fault_env_.reset(new CompositeEnvWrapper(env_, fault_fs_));
  }

  std::string GetManifestNameFromLiveFiles() {
    std::vector<std::string> live_files;
    uint64_t manifest_size;

    Status s = dbfull()->GetLiveFiles(live_files, &manifest_size, false);
    if (!s.ok()) {
      return "";
    }
    for (auto& file : live_files) {
      uint64_t num = 0;
      FileType type;
      if (ParseFileName(file, &num, &type) && type == kDescriptorFile) {
        return file;
      }
    }
    return "";
  }

  std::shared_ptr<FaultInjectionTestFS> fault_fs_;
  std::unique_ptr<Env> fault_env_;
};

class ErrorHandlerFSListener : public EventListener {
 public:
  ErrorHandlerFSListener()
      : mutex_(),
        cv_(&mutex_),
        no_auto_recovery_(false),
        recovery_complete_(false),
        file_creation_started_(false),
        override_bg_error_(false),
        file_count_(0),
        fault_fs_(nullptr) {}
  ~ErrorHandlerFSListener() {
    file_creation_error_.PermitUncheckedError();
    bg_error_.PermitUncheckedError();
  }

  void OnTableFileCreationStarted(
      const TableFileCreationBriefInfo& /*ti*/) override {
    InstrumentedMutexLock l(&mutex_);
    file_creation_started_ = true;
    if (file_count_ > 0) {
      if (--file_count_ == 0) {
        fault_fs_->SetFilesystemActive(false, file_creation_error_);
        file_creation_error_ = IOStatus::OK();
      }
    }
    cv_.SignalAll();
  }

  void OnErrorRecoveryBegin(BackgroundErrorReason /*reason*/, Status bg_error,
                            bool* auto_recovery) override {
    bg_error.PermitUncheckedError();
    if (*auto_recovery && no_auto_recovery_) {
      *auto_recovery = false;
    }
  }

  void OnErrorRecoveryCompleted(Status old_bg_error) override {
    InstrumentedMutexLock l(&mutex_);
    recovery_complete_ = true;
    cv_.SignalAll();
    old_bg_error.PermitUncheckedError();
  }

  bool WaitForRecovery(uint64_t /*abs_time_us*/) {
    InstrumentedMutexLock l(&mutex_);
    while (!recovery_complete_) {
      cv_.Wait(/*abs_time_us*/);
    }
    if (recovery_complete_) {
      recovery_complete_ = false;
      return true;
    }
    return false;
  }

  void WaitForTableFileCreationStarted(uint64_t /*abs_time_us*/) {
    InstrumentedMutexLock l(&mutex_);
    while (!file_creation_started_) {
      cv_.Wait(/*abs_time_us*/);
    }
    file_creation_started_ = false;
  }

  void OnBackgroundError(BackgroundErrorReason /*reason*/,
                         Status* bg_error) override {
    if (override_bg_error_) {
      *bg_error = bg_error_;
      override_bg_error_ = false;
    }
  }

  void EnableAutoRecovery(bool enable = true) { no_auto_recovery_ = !enable; }

  void OverrideBGError(Status bg_err) {
    bg_error_ = bg_err;
    override_bg_error_ = true;
  }

  void InjectFileCreationError(FaultInjectionTestFS* fs, int file_count,
                               IOStatus io_s) {
    fault_fs_ = fs;
    file_count_ = file_count;
    file_creation_error_ = io_s;
  }

 private:
  InstrumentedMutex mutex_;
  InstrumentedCondVar cv_;
  bool no_auto_recovery_;
  bool recovery_complete_;
  bool file_creation_started_;
  bool override_bg_error_;
  int file_count_;
  IOStatus file_creation_error_;
  Status bg_error_;
  FaultInjectionTestFS* fault_fs_;
};

TEST_F(DBErrorHandlingFSTest, FLushWriteError) {
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  Status s;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);

  ASSERT_OK(Put(Key(0), "val"));
  SyncPoint::GetInstance()->SetCallBack("FlushJob::Start", [&](void*) {
    fault_fs_->SetFilesystemActive(false, IOStatus::NoSpace("Out of space"));
  });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), CABINDB_NAMESPACE::Status::Severity::kHardError);
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_EQ(s, Status::OK());

  Reopen(options);
  ASSERT_EQ("val", Get(Key(0)));
  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, FLushWritRetryableError) {
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 0;
  Status s;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  ASSERT_OK(Put(Key(1), "val1"));
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeFinishBuildTable",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), CABINDB_NAMESPACE::Status::Severity::kHardError);
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_OK(s);
  Reopen(options);
  ASSERT_EQ("val1", Get(Key(1)));

  ASSERT_OK(Put(Key(2), "val2"));
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeSyncTable",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), CABINDB_NAMESPACE::Status::Severity::kHardError);
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_OK(s);
  Reopen(options);
  ASSERT_EQ("val2", Get(Key(2)));

  ASSERT_OK(Put(Key(3), "val3"));
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeCloseTableFile",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), CABINDB_NAMESPACE::Status::Severity::kHardError);
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_OK(s);
  Reopen(options);
  ASSERT_EQ("val3", Get(Key(3)));

  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, FLushWritNoWALRetryableError1) {
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 0;
  Status s;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  WriteOptions wo = WriteOptions();
  wo.disableWAL = true;
  ASSERT_OK(Put(Key(1), "val1", wo));
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeFinishBuildTable",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_OK(Put(Key(2), "val2", wo));
  ASSERT_EQ(s.severity(), CABINDB_NAMESPACE::Status::Severity::kSoftError);
  ASSERT_EQ("val2", Get(Key(2)));
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ("val1", Get(Key(1)));
  ASSERT_EQ("val2", Get(Key(2)));
  ASSERT_OK(Put(Key(3), "val3", wo));
  ASSERT_EQ("val3", Get(Key(3)));
  s = Flush();
  ASSERT_OK(s);
  ASSERT_EQ("val3", Get(Key(3)));

  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, FLushWritNoWALRetryableError2) {
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 0;
  Status s;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  WriteOptions wo = WriteOptions();
  wo.disableWAL = true;

  ASSERT_OK(Put(Key(1), "val1", wo));
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeSyncTable",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_OK(Put(Key(2), "val2", wo));
  ASSERT_EQ(s.severity(), CABINDB_NAMESPACE::Status::Severity::kSoftError);
  ASSERT_EQ("val2", Get(Key(2)));
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ("val1", Get(Key(1)));
  ASSERT_EQ("val2", Get(Key(2)));
  ASSERT_OK(Put(Key(3), "val3", wo));
  ASSERT_EQ("val3", Get(Key(3)));
  s = Flush();
  ASSERT_OK(s);
  ASSERT_EQ("val3", Get(Key(3)));

  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, FLushWritNoWALRetryableError3) {
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 0;
  Status s;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  WriteOptions wo = WriteOptions();
  wo.disableWAL = true;

  ASSERT_OK(Put(Key(1), "val1", wo));
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeCloseTableFile",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_OK(Put(Key(2), "val2", wo));
  ASSERT_EQ(s.severity(), CABINDB_NAMESPACE::Status::Severity::kSoftError);
  ASSERT_EQ("val2", Get(Key(2)));
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ("val1", Get(Key(1)));
  ASSERT_EQ("val2", Get(Key(2)));
  ASSERT_OK(Put(Key(3), "val3", wo));
  ASSERT_EQ("val3", Get(Key(3)));
  s = Flush();
  ASSERT_OK(s);
  ASSERT_EQ("val3", Get(Key(3)));

  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, ManifestWriteError) {
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  Status s;
  std::string old_manifest;
  std::string new_manifest;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);
  old_manifest = GetManifestNameFromLiveFiles();

  ASSERT_OK(Put(Key(0), "val"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(Key(1), "val"));
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest", [&](void*) {
        fault_fs_->SetFilesystemActive(false,
                                       IOStatus::NoSpace("Out of space"));
      });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), CABINDB_NAMESPACE::Status::Severity::kHardError);
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_EQ(s, Status::OK());

  new_manifest = GetManifestNameFromLiveFiles();
  ASSERT_NE(new_manifest, old_manifest);

  Reopen(options);
  ASSERT_EQ("val", Get(Key(0)));
  ASSERT_EQ("val", Get(Key(1)));
  Close();
}

TEST_F(DBErrorHandlingFSTest, ManifestWriteRetryableError) {
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 0;
  Status s;
  std::string old_manifest;
  std::string new_manifest;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);
  old_manifest = GetManifestNameFromLiveFiles();

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  ASSERT_OK(Put(Key(0), "val"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(Key(1), "val"));
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), CABINDB_NAMESPACE::Status::Severity::kHardError);
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_EQ(s, Status::OK());

  new_manifest = GetManifestNameFromLiveFiles();
  ASSERT_NE(new_manifest, old_manifest);

  Reopen(options);
  ASSERT_EQ("val", Get(Key(0)));
  ASSERT_EQ("val", Get(Key(1)));
  Close();
}

TEST_F(DBErrorHandlingFSTest, DoubleManifestWriteError) {
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  Status s;
  std::string old_manifest;
  std::string new_manifest;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);
  old_manifest = GetManifestNameFromLiveFiles();

  ASSERT_OK(Put(Key(0), "val"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(Key(1), "val"));
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest", [&](void*) {
        fault_fs_->SetFilesystemActive(false,
                                       IOStatus::NoSpace("Out of space"));
      });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), CABINDB_NAMESPACE::Status::Severity::kHardError);
  fault_fs_->SetFilesystemActive(true);

  // This Resume() will attempt to create a new manifest file and fail again
  s = dbfull()->Resume();
  ASSERT_EQ(s.severity(), CABINDB_NAMESPACE::Status::Severity::kHardError);
  fault_fs_->SetFilesystemActive(true);
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();

  // A successful Resume() will create a new manifest file
  s = dbfull()->Resume();
  ASSERT_EQ(s, Status::OK());

  new_manifest = GetManifestNameFromLiveFiles();
  ASSERT_NE(new_manifest, old_manifest);

  Reopen(options);
  ASSERT_EQ("val", Get(Key(0)));
  ASSERT_EQ("val", Get(Key(1)));
  Close();
}

TEST_F(DBErrorHandlingFSTest, CompactionManifestWriteError) {
  if (mem_env_ != nullptr) {
    CABINDB_GTEST_SKIP("Test requires non-mock environment");
    return;
  }
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.level0_file_num_compaction_trigger = 2;
  options.listeners.emplace_back(listener);
  Status s;
  std::string old_manifest;
  std::string new_manifest;
  std::atomic<bool> fail_manifest(false);
  DestroyAndReopen(options);
  old_manifest = GetManifestNameFromLiveFiles();

  ASSERT_OK(Put(Key(0), "val"));
  ASSERT_OK(Put(Key(2), "val"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());

  CABINDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      // Wait for flush of 2nd L0 file before starting compaction
      {{"DBImpl::FlushMemTable:FlushMemTableFinished",
        "BackgroundCallCompaction:0"},
       // Wait for compaction to detect manifest write error
       {"BackgroundCallCompaction:1", "CompactionManifestWriteError:0"},
       // Make compaction thread wait for error to be cleared
       {"CompactionManifestWriteError:1",
        "DBImpl::BackgroundCallCompaction:FoundObsoleteFiles"},
       // Wait for DB instance to clear bg_error before calling
       // TEST_WaitForCompact
       {"SstFileManagerImpl::ErrorCleared", "CompactionManifestWriteError:2"}});
  // trigger manifest write failure in compaction thread
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void*) { fail_manifest.store(true); });
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest", [&](void*) {
        if (fail_manifest.load()) {
          fault_fs_->SetFilesystemActive(false,
                                         IOStatus::NoSpace("Out of space"));
        }
      });
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(Key(1), "val"));
  // This Flush will trigger a compaction, which will fail when appending to
  // the manifest
  s = Flush();
  ASSERT_EQ(s, Status::OK());

  TEST_SYNC_POINT("CompactionManifestWriteError:0");
  // Clear all errors so when the compaction is retried, it will succeed
  fault_fs_->SetFilesystemActive(true);
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  TEST_SYNC_POINT("CompactionManifestWriteError:1");
  TEST_SYNC_POINT("CompactionManifestWriteError:2");

  s = dbfull()->TEST_WaitForCompact();
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_EQ(s, Status::OK());

  new_manifest = GetManifestNameFromLiveFiles();
  ASSERT_NE(new_manifest, old_manifest);
  Reopen(options);
  ASSERT_EQ("val", Get(Key(0)));
  ASSERT_EQ("val", Get(Key(1)));
  ASSERT_EQ("val", Get(Key(2)));
  Close();
}

TEST_F(DBErrorHandlingFSTest, CompactionManifestWriteRetryableError) {
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.level0_file_num_compaction_trigger = 2;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 0;
  Status s;
  std::string old_manifest;
  std::string new_manifest;
  std::atomic<bool> fail_manifest(false);
  DestroyAndReopen(options);
  old_manifest = GetManifestNameFromLiveFiles();

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  ASSERT_OK(Put(Key(0), "val"));
  ASSERT_OK(Put(Key(2), "val"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());

  listener->OverrideBGError(Status(error_msg, Status::Severity::kHardError));
  listener->EnableAutoRecovery(false);
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      // Wait for flush of 2nd L0 file before starting compaction
      {{"DBImpl::FlushMemTable:FlushMemTableFinished",
        "BackgroundCallCompaction:0"},
       // Wait for compaction to detect manifest write error
       {"BackgroundCallCompaction:1", "CompactionManifestWriteError:0"},
       // Make compaction thread wait for error to be cleared
       {"CompactionManifestWriteError:1",
        "DBImpl::BackgroundCallCompaction:FoundObsoleteFiles"}});
  // trigger manifest write failure in compaction thread
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void*) { fail_manifest.store(true); });
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest", [&](void*) {
        if (fail_manifest.load()) {
          fault_fs_->SetFilesystemActive(false, error_msg);
        }
      });
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(Key(1), "val"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());

  TEST_SYNC_POINT("CompactionManifestWriteError:0");
  TEST_SYNC_POINT("CompactionManifestWriteError:1");

  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s.severity(), CABINDB_NAMESPACE::Status::Severity::kHardError);

  fault_fs_->SetFilesystemActive(true);
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
  s = dbfull()->Resume();
  ASSERT_EQ(s, Status::OK());

  new_manifest = GetManifestNameFromLiveFiles();
  ASSERT_NE(new_manifest, old_manifest);

  Reopen(options);
  ASSERT_EQ("val", Get(Key(0)));
  ASSERT_EQ("val", Get(Key(1)));
  ASSERT_EQ("val", Get(Key(2)));
  Close();
}

TEST_F(DBErrorHandlingFSTest, CompactionWriteError) {
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.level0_file_num_compaction_trigger = 2;
  options.listeners.emplace_back(listener);
  Status s;
  DestroyAndReopen(options);

  ASSERT_OK(Put(Key(0), "va;"));
  ASSERT_OK(Put(Key(2), "va;"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());

  listener->OverrideBGError(
      Status(Status::NoSpace(), Status::Severity::kHardError));
  listener->EnableAutoRecovery(false);
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::FlushMemTable:FlushMemTableFinished",
        "BackgroundCallCompaction:0"}});
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void*) {
        fault_fs_->SetFilesystemActive(false,
                                       IOStatus::NoSpace("Out of space"));
      });
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(Key(1), "val"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());

  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s.severity(), CABINDB_NAMESPACE::Status::Severity::kHardError);

  fault_fs_->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_EQ(s, Status::OK());
  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, CompactionWriteRetryableError) {
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.level0_file_num_compaction_trigger = 2;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 0;
  Status s;
  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  ASSERT_OK(Put(Key(0), "va;"));
  ASSERT_OK(Put(Key(2), "va;"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());

  listener->OverrideBGError(Status(error_msg, Status::Severity::kHardError));
  listener->EnableAutoRecovery(false);
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::FlushMemTable:FlushMemTableFinished",
        "BackgroundCallCompaction:0"}});
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::OpenCompactionOutputFile",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(Key(1), "val"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());

  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s.severity(), CABINDB_NAMESPACE::Status::Severity::kSoftError);

  fault_fs_->SetFilesystemActive(true);
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
  s = dbfull()->Resume();
  ASSERT_EQ(s, Status::OK());
  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, CorruptionError) {
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.level0_file_num_compaction_trigger = 2;
  Status s;
  DestroyAndReopen(options);

  ASSERT_OK(Put(Key(0), "va;"));
  ASSERT_OK(Put(Key(2), "va;"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());

  CABINDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::FlushMemTable:FlushMemTableFinished",
        "BackgroundCallCompaction:0"}});
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void*) {
        fault_fs_->SetFilesystemActive(false,
                                       IOStatus::Corruption("Corruption"));
      });
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(Key(1), "val"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());

  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s.severity(),
            CABINDB_NAMESPACE::Status::Severity::kUnrecoverableError);

  fault_fs_->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_NE(s, Status::OK());
  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, AutoRecoverFlushError) {
  if (mem_env_ != nullptr) {
    CABINDB_GTEST_SKIP("Test requires non-mock environment");
    return;
  }
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  Status s;

  listener->EnableAutoRecovery();
  DestroyAndReopen(options);

  ASSERT_OK(Put(Key(0), "val"));
  SyncPoint::GetInstance()->SetCallBack("FlushJob::Start", [&](void*) {
    fault_fs_->SetFilesystemActive(false, IOStatus::NoSpace("Out of space"));
  });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), CABINDB_NAMESPACE::Status::Severity::kHardError);
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  ASSERT_EQ(listener->WaitForRecovery(5000000), true);

  s = Put(Key(1), "val");
  ASSERT_EQ(s, Status::OK());

  Reopen(options);
  ASSERT_EQ("val", Get(Key(0)));
  ASSERT_EQ("val", Get(Key(1)));
  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, FailRecoverFlushError) {
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  Status s;

  listener->EnableAutoRecovery();
  DestroyAndReopen(options);

  ASSERT_OK(Put(Key(0), "val"));
  SyncPoint::GetInstance()->SetCallBack("FlushJob::Start", [&](void*) {
    fault_fs_->SetFilesystemActive(false, IOStatus::NoSpace("Out of space"));
  });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), CABINDB_NAMESPACE::Status::Severity::kHardError);
  // We should be able to shutdown the database while auto recovery is going
  // on in the background
  Close();
  DestroyDB(dbname_, options);
}

TEST_F(DBErrorHandlingFSTest, WALWriteError) {
  if (mem_env_ != nullptr) {
    CABINDB_GTEST_SKIP("Test requires non-mock environment");
    return;
  }
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.writable_file_max_buffer_size = 32768;
  options.listeners.emplace_back(listener);
  Status s;
  Random rnd(301);

  listener->EnableAutoRecovery();
  DestroyAndReopen(options);

  {
    WriteBatch batch;

    for (auto i = 0; i < 100; ++i) {
      ASSERT_OK(batch.Put(Key(i), rnd.RandomString(1024)));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_EQ(dbfull()->Write(wopts, &batch), Status::OK());
  };

  {
    WriteBatch batch;
    int write_error = 0;

    for (auto i = 100; i < 199; ++i) {
      ASSERT_OK(batch.Put(Key(i), rnd.RandomString(1024)));
    }

    SyncPoint::GetInstance()->SetCallBack(
        "WritableFileWriter::Append:BeforePrepareWrite", [&](void*) {
          write_error++;
          if (write_error > 2) {
            fault_fs_->SetFilesystemActive(false,
                                           IOStatus::NoSpace("Out of space"));
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
    WriteOptions wopts;
    wopts.sync = true;
    s = dbfull()->Write(wopts, &batch);
    ASSERT_EQ(s, s.NoSpace());
  }
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  ASSERT_EQ(listener->WaitForRecovery(5000000), true);
  for (auto i = 0; i < 199; ++i) {
    if (i < 100) {
      ASSERT_NE(Get(Key(i)), "NOT_FOUND");
    } else {
      ASSERT_EQ(Get(Key(i)), "NOT_FOUND");
    }
  }
  Reopen(options);
  for (auto i = 0; i < 199; ++i) {
    if (i < 100) {
      ASSERT_NE(Get(Key(i)), "NOT_FOUND");
    } else {
      ASSERT_EQ(Get(Key(i)), "NOT_FOUND");
    }
  }
  Close();
}

TEST_F(DBErrorHandlingFSTest, WALWriteRetryableError) {
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.writable_file_max_buffer_size = 32768;
  options.listeners.emplace_back(listener);
  options.paranoid_checks = true;
  options.max_bgerror_resume_count = 0;
  Status s;
  Random rnd(301);

  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  // For the first batch, write is successful, require sync
  {
    WriteBatch batch;

    for (auto i = 0; i < 100; ++i) {
      ASSERT_OK(batch.Put(Key(i), rnd.RandomString(1024)));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_EQ(dbfull()->Write(wopts, &batch), Status::OK());
  };

  // For the second batch, the first 2 file Append are successful, then the
  // following Append fails due to file system retryable IOError.
  {
    WriteBatch batch;
    int write_error = 0;

    for (auto i = 100; i < 200; ++i) {
      ASSERT_OK(batch.Put(Key(i), rnd.RandomString(1024)));
    }

    SyncPoint::GetInstance()->SetCallBack(
        "WritableFileWriter::Append:BeforePrepareWrite", [&](void*) {
          write_error++;
          if (write_error > 2) {
            fault_fs_->SetFilesystemActive(false, error_msg);
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
    WriteOptions wopts;
    wopts.sync = true;
    s = dbfull()->Write(wopts, &batch);
    ASSERT_EQ(true, s.IsIOError());
  }
  fault_fs_->SetFilesystemActive(true);
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();

  // Data in corrupted WAL are not stored
  for (auto i = 0; i < 199; ++i) {
    if (i < 100) {
      ASSERT_NE(Get(Key(i)), "NOT_FOUND");
    } else {
      ASSERT_EQ(Get(Key(i)), "NOT_FOUND");
    }
  }

  // Resume and write a new batch, should be in the WAL
  s = dbfull()->Resume();
  ASSERT_EQ(s, Status::OK());
  {
    WriteBatch batch;

    for (auto i = 200; i < 300; ++i) {
      ASSERT_OK(batch.Put(Key(i), rnd.RandomString(1024)));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_EQ(dbfull()->Write(wopts, &batch), Status::OK());
  };

  Reopen(options);
  for (auto i = 0; i < 300; ++i) {
    if (i < 100 || i >= 200) {
      ASSERT_NE(Get(Key(i)), "NOT_FOUND");
    } else {
      ASSERT_EQ(Get(Key(i)), "NOT_FOUND");
    }
  }
  Close();
}

TEST_F(DBErrorHandlingFSTest, MultiCFWALWriteError) {
  if (mem_env_ != nullptr) {
    CABINDB_GTEST_SKIP("Test requires non-mock environment");
    return;
  }
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.writable_file_max_buffer_size = 32768;
  options.listeners.emplace_back(listener);
  Status s;
  Random rnd(301);

  listener->EnableAutoRecovery();
  CreateAndReopenWithCF({"one", "two", "three"}, options);

  {
    WriteBatch batch;

    for (auto i = 1; i < 4; ++i) {
      for (auto j = 0; j < 100; ++j) {
        ASSERT_OK(batch.Put(handles_[i], Key(j), rnd.RandomString(1024)));
      }
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_EQ(dbfull()->Write(wopts, &batch), Status::OK());
  };

  {
    WriteBatch batch;
    int write_error = 0;

    // Write to one CF
    for (auto i = 100; i < 199; ++i) {
      ASSERT_OK(batch.Put(handles_[2], Key(i), rnd.RandomString(1024)));
    }

    SyncPoint::GetInstance()->SetCallBack(
        "WritableFileWriter::Append:BeforePrepareWrite", [&](void*) {
          write_error++;
          if (write_error > 2) {
            fault_fs_->SetFilesystemActive(false,
                                           IOStatus::NoSpace("Out of space"));
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
    WriteOptions wopts;
    wopts.sync = true;
    s = dbfull()->Write(wopts, &batch);
    ASSERT_EQ(s, s.NoSpace());
  }
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  ASSERT_EQ(listener->WaitForRecovery(5000000), true);

  for (auto i = 1; i < 4; ++i) {
    // Every CF should have been flushed
    ASSERT_EQ(NumTableFilesAtLevel(0, i), 1);
  }

  for (auto i = 1; i < 4; ++i) {
    for (auto j = 0; j < 199; ++j) {
      if (j < 100) {
        ASSERT_NE(Get(i, Key(j)), "NOT_FOUND");
      } else {
        ASSERT_EQ(Get(i, Key(j)), "NOT_FOUND");
      }
    }
  }
  ReopenWithColumnFamilies({"default", "one", "two", "three"}, options);
  for (auto i = 1; i < 4; ++i) {
    for (auto j = 0; j < 199; ++j) {
      if (j < 100) {
        ASSERT_NE(Get(i, Key(j)), "NOT_FOUND");
      } else {
        ASSERT_EQ(Get(i, Key(j)), "NOT_FOUND");
      }
    }
  }
  Close();
}

TEST_F(DBErrorHandlingFSTest, MultiDBCompactionError) {
  if (mem_env_ != nullptr) {
    CABINDB_GTEST_SKIP("Test requires non-mock environment");
    return;
  }
  FaultInjectionTestEnv* def_env = new FaultInjectionTestEnv(env_);
  std::vector<std::unique_ptr<Env>> fault_envs;
  std::vector<FaultInjectionTestFS*> fault_fs;
  std::vector<Options> options;
  std::vector<std::shared_ptr<ErrorHandlerFSListener>> listener;
  std::vector<DB*> db;
  std::shared_ptr<SstFileManager> sfm(NewSstFileManager(def_env));
  int kNumDbInstances = 3;
  Random rnd(301);

  for (auto i = 0; i < kNumDbInstances; ++i) {
    listener.emplace_back(new ErrorHandlerFSListener());
    options.emplace_back(GetDefaultOptions());
    fault_fs.emplace_back(new FaultInjectionTestFS(env_->GetFileSystem()));
    std::shared_ptr<FileSystem> fs(fault_fs.back());
    fault_envs.emplace_back(new CompositeEnvWrapper(def_env, fs));
    options[i].env = fault_envs.back().get();
    options[i].create_if_missing = true;
    options[i].level0_file_num_compaction_trigger = 2;
    options[i].writable_file_max_buffer_size = 32768;
    options[i].listeners.emplace_back(listener[i]);
    options[i].sst_file_manager = sfm;
    DB* dbptr;
    char buf[16];

    listener[i]->EnableAutoRecovery();
    // Setup for returning error for the 3rd SST, which would be level 1
    listener[i]->InjectFileCreationError(fault_fs[i], 3,
                                         IOStatus::NoSpace("Out of space"));
    snprintf(buf, sizeof(buf), "_%d", i);
    DestroyDB(dbname_ + std::string(buf), options[i]);
    ASSERT_EQ(DB::Open(options[i], dbname_ + std::string(buf), &dbptr),
              Status::OK());
    db.emplace_back(dbptr);
  }

  for (auto i = 0; i < kNumDbInstances; ++i) {
    WriteBatch batch;

    for (auto j = 0; j <= 100; ++j) {
      ASSERT_OK(batch.Put(Key(j), rnd.RandomString(1024)));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_EQ(db[i]->Write(wopts, &batch), Status::OK());
    ASSERT_EQ(db[i]->Flush(FlushOptions()), Status::OK());
  }

  def_env->SetFilesystemActive(false, Status::NoSpace("Out of space"));
  for (auto i = 0; i < kNumDbInstances; ++i) {
    WriteBatch batch;

    // Write to one CF
    for (auto j = 100; j < 199; ++j) {
      ASSERT_OK(batch.Put(Key(j), rnd.RandomString(1024)));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_EQ(db[i]->Write(wopts, &batch), Status::OK());
    ASSERT_EQ(db[i]->Flush(FlushOptions()), Status::OK());
  }

  for (auto i = 0; i < kNumDbInstances; ++i) {
    Status s = static_cast<DBImpl*>(db[i])->TEST_WaitForCompact(true);
    ASSERT_EQ(s.severity(), Status::Severity::kSoftError);
    fault_fs[i]->SetFilesystemActive(true);
  }

  def_env->SetFilesystemActive(true);
  for (auto i = 0; i < kNumDbInstances; ++i) {
    std::string prop;
    ASSERT_EQ(listener[i]->WaitForRecovery(5000000), true);
    ASSERT_EQ(static_cast<DBImpl*>(db[i])->TEST_WaitForCompact(true),
              Status::OK());
    EXPECT_TRUE(db[i]->GetProperty(
        "cabindb.num-files-at-level" + NumberToString(0), &prop));
    EXPECT_EQ(atoi(prop.c_str()), 0);
    EXPECT_TRUE(db[i]->GetProperty(
        "cabindb.num-files-at-level" + NumberToString(1), &prop));
    EXPECT_EQ(atoi(prop.c_str()), 1);
  }

  for (auto i = 0; i < kNumDbInstances; ++i) {
    char buf[16];
    snprintf(buf, sizeof(buf), "_%d", i);
    delete db[i];
    fault_fs[i]->SetFilesystemActive(true);
    if (getenv("KEEP_DB")) {
      printf("DB is still at %s%s\n", dbname_.c_str(), buf);
    } else {
      Status s = DestroyDB(dbname_ + std::string(buf), options[i]);
    }
  }
  options.clear();
  sfm.reset();
  delete def_env;
}

TEST_F(DBErrorHandlingFSTest, MultiDBVariousErrors) {
  if (mem_env_ != nullptr) {
    CABINDB_GTEST_SKIP("Test requires non-mock environment");
    return;
  }
  FaultInjectionTestEnv* def_env = new FaultInjectionTestEnv(env_);
  std::vector<std::unique_ptr<Env>> fault_envs;
  std::vector<FaultInjectionTestFS*> fault_fs;
  std::vector<Options> options;
  std::vector<std::shared_ptr<ErrorHandlerFSListener>> listener;
  std::vector<DB*> db;
  std::shared_ptr<SstFileManager> sfm(NewSstFileManager(def_env));
  int kNumDbInstances = 3;
  Random rnd(301);

  for (auto i = 0; i < kNumDbInstances; ++i) {
    listener.emplace_back(new ErrorHandlerFSListener());
    options.emplace_back(GetDefaultOptions());
    fault_fs.emplace_back(new FaultInjectionTestFS(env_->GetFileSystem()));
    std::shared_ptr<FileSystem> fs(fault_fs.back());
    fault_envs.emplace_back(new CompositeEnvWrapper(def_env, fs));
    options[i].env = fault_envs.back().get();
    options[i].create_if_missing = true;
    options[i].level0_file_num_compaction_trigger = 2;
    options[i].writable_file_max_buffer_size = 32768;
    options[i].listeners.emplace_back(listener[i]);
    options[i].sst_file_manager = sfm;
    DB* dbptr;
    char buf[16];

    listener[i]->EnableAutoRecovery();
    switch (i) {
      case 0:
        // Setup for returning error for the 3rd SST, which would be level 1
        listener[i]->InjectFileCreationError(fault_fs[i], 3,
                                             IOStatus::NoSpace("Out of space"));
        break;
      case 1:
        // Setup for returning error after the 1st SST, which would result
        // in a hard error
        listener[i]->InjectFileCreationError(fault_fs[i], 2,
                                             IOStatus::NoSpace("Out of space"));
        break;
      default:
        break;
    }
    snprintf(buf, sizeof(buf), "_%d", i);
    DestroyDB(dbname_ + std::string(buf), options[i]);
    ASSERT_EQ(DB::Open(options[i], dbname_ + std::string(buf), &dbptr),
              Status::OK());
    db.emplace_back(dbptr);
  }

  for (auto i = 0; i < kNumDbInstances; ++i) {
    WriteBatch batch;

    for (auto j = 0; j <= 100; ++j) {
      ASSERT_OK(batch.Put(Key(j), rnd.RandomString(1024)));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_EQ(db[i]->Write(wopts, &batch), Status::OK());
    ASSERT_EQ(db[i]->Flush(FlushOptions()), Status::OK());
  }

  def_env->SetFilesystemActive(false, Status::NoSpace("Out of space"));
  for (auto i = 0; i < kNumDbInstances; ++i) {
    WriteBatch batch;

    // Write to one CF
    for (auto j = 100; j < 199; ++j) {
      ASSERT_OK(batch.Put(Key(j), rnd.RandomString(1024)));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_EQ(db[i]->Write(wopts, &batch), Status::OK());
    if (i != 1) {
      ASSERT_EQ(db[i]->Flush(FlushOptions()), Status::OK());
    } else {
      ASSERT_EQ(db[i]->Flush(FlushOptions()), Status::NoSpace());
    }
  }

  for (auto i = 0; i < kNumDbInstances; ++i) {
    Status s = static_cast<DBImpl*>(db[i])->TEST_WaitForCompact(true);
    switch (i) {
      case 0:
        ASSERT_EQ(s.severity(), Status::Severity::kSoftError);
        break;
      case 1:
        ASSERT_EQ(s.severity(), Status::Severity::kHardError);
        break;
      case 2:
        ASSERT_EQ(s, Status::OK());
        break;
    }
    fault_fs[i]->SetFilesystemActive(true);
  }

  def_env->SetFilesystemActive(true);
  for (auto i = 0; i < kNumDbInstances; ++i) {
    std::string prop;
    if (i < 2) {
      ASSERT_EQ(listener[i]->WaitForRecovery(5000000), true);
    }
    if (i == 1) {
      ASSERT_EQ(static_cast<DBImpl*>(db[i])->TEST_WaitForCompact(true),
                Status::OK());
    }
    EXPECT_TRUE(db[i]->GetProperty(
        "cabindb.num-files-at-level" + NumberToString(0), &prop));
    EXPECT_EQ(atoi(prop.c_str()), 0);
    EXPECT_TRUE(db[i]->GetProperty(
        "cabindb.num-files-at-level" + NumberToString(1), &prop));
    EXPECT_EQ(atoi(prop.c_str()), 1);
  }

  for (auto i = 0; i < kNumDbInstances; ++i) {
    char buf[16];
    snprintf(buf, sizeof(buf), "_%d", i);
    fault_fs[i]->SetFilesystemActive(true);
    delete db[i];
    if (getenv("KEEP_DB")) {
      printf("DB is still at %s%s\n", dbname_.c_str(), buf);
    } else {
      DestroyDB(dbname_ + std::string(buf), options[i]);
    }
  }
  options.clear();
  delete def_env;
}

// When Put the KV-pair, the write option is set to disable WAL.
// If retryable error happens in this condition, map the bg error
// to soft error and trigger auto resume. During auto resume, SwitchMemtable
// is disabled to avoid small SST tables. Write can still be applied before
// the bg error is cleaned unless the memtable is full.
TEST_F(DBErrorHandlingFSTest, FLushWritNoWALRetryableeErrorAutoRecover1) {
  // Activate the FS before the first resume
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 2;
  options.bgerror_resume_retry_interval = 100000;  // 0.1 second
  Status s;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  WriteOptions wo = WriteOptions();
  wo.disableWAL = true;
  ASSERT_OK(Put(Key(1), "val1", wo));
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"RecoverFromRetryableBGIOError:LoopOut",
        "FLushWritNoWALRetryableeErrorAutoRecover1:1"}});
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeFinishBuildTable",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });

  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ("val1", Get(Key(1)));
  ASSERT_EQ(s.severity(), CABINDB_NAMESPACE::Status::Severity::kSoftError);
  TEST_SYNC_POINT("FLushWritNoWALRetryableeErrorAutoRecover1:1");
  ASSERT_EQ("val1", Get(Key(1)));
  ASSERT_EQ("val1", Get(Key(1)));
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  ASSERT_OK(Put(Key(2), "val2", wo));
  s = Flush();
  // Since auto resume fails, the bg error is not cleand, flush will
  // return the bg_error set before.
  ASSERT_EQ(s.severity(), CABINDB_NAMESPACE::Status::Severity::kSoftError);
  ASSERT_EQ("val2", Get(Key(2)));

  // call auto resume
  s = dbfull()->Resume();
  ASSERT_EQ(s, Status::OK());
  ASSERT_OK(Put(Key(3), "val3", wo));
  s = Flush();
  // After resume is successful, the flush should be ok.
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ("val3", Get(Key(3)));
  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, FLushWritNoWALRetryableeErrorAutoRecover2) {
  // Activate the FS before the first resume
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 2;
  options.bgerror_resume_retry_interval = 100000;  // 0.1 second
  Status s;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  WriteOptions wo = WriteOptions();
  wo.disableWAL = true;
  ASSERT_OK(Put(Key(1), "val1", wo));
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeFinishBuildTable",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });

  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ("val1", Get(Key(1)));
  ASSERT_EQ(s.severity(), CABINDB_NAMESPACE::Status::Severity::kSoftError);
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  ASSERT_EQ(listener->WaitForRecovery(5000000), true);
  ASSERT_EQ("val1", Get(Key(1)));
  ASSERT_OK(Put(Key(2), "val2", wo));
  s = Flush();
  // Since auto resume is successful, the bg error is cleaned, flush will
  // be successful.
  ASSERT_OK(s);
  ASSERT_EQ("val2", Get(Key(2)));
  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, DISABLED_FLushWritRetryableeErrorAutoRecover1) {
  // Fail the first resume and make the second resume successful
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 2;
  options.bgerror_resume_retry_interval = 100000;  // 0.1 second
  Status s;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  ASSERT_OK(Put(Key(1), "val1"));
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"RecoverFromRetryableBGIOError:BeforeWait0",
        "FLushWritRetryableeErrorAutoRecover1:0"},
       {"FLushWritRetryableeErrorAutoRecover1:1",
        "RecoverFromRetryableBGIOError:BeforeWait1"},
       {"RecoverFromRetryableBGIOError:RecoverSuccess",
        "FLushWritRetryableeErrorAutoRecover1:2"}});
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeFinishBuildTable",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), CABINDB_NAMESPACE::Status::Severity::kHardError);
  TEST_SYNC_POINT("FLushWritRetryableeErrorAutoRecover1:0");
  fault_fs_->SetFilesystemActive(true);
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  TEST_SYNC_POINT("FLushWritRetryableeErrorAutoRecover1:1");
  TEST_SYNC_POINT("FLushWritRetryableeErrorAutoRecover1:2");
  SyncPoint::GetInstance()->DisableProcessing();

  ASSERT_EQ("val1", Get(Key(1)));
  Reopen(options);
  ASSERT_EQ("val1", Get(Key(1)));
  ASSERT_OK(Put(Key(2), "val2"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ("val2", Get(Key(2)));

  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, FLushWritRetryableeErrorAutoRecover2) {
  // Activate the FS before the first resume
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 2;
  options.bgerror_resume_retry_interval = 100000;  // 0.1 second
  Status s;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  ASSERT_OK(Put(Key(1), "val1"));
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeFinishBuildTable",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });

  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), CABINDB_NAMESPACE::Status::Severity::kHardError);
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  ASSERT_EQ(listener->WaitForRecovery(5000000), true);

  ASSERT_EQ("val1", Get(Key(1)));
  Reopen(options);
  ASSERT_EQ("val1", Get(Key(1)));
  ASSERT_OK(Put(Key(2), "val2"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ("val2", Get(Key(2)));

  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, FLushWritRetryableeErrorAutoRecover3) {
  // Fail all the resume and let user to resume
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 2;
  options.bgerror_resume_retry_interval = 100000;  // 0.1 second
  Status s;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  ASSERT_OK(Put(Key(1), "val1"));
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"FLushWritRetryableeErrorAutoRecover3:0",
        "RecoverFromRetryableBGIOError:BeforeStart"},
       {"RecoverFromRetryableBGIOError:LoopOut",
        "FLushWritRetryableeErrorAutoRecover3:1"}});
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeFinishBuildTable",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), CABINDB_NAMESPACE::Status::Severity::kHardError);
  TEST_SYNC_POINT("FLushWritRetryableeErrorAutoRecover3:0");
  TEST_SYNC_POINT("FLushWritRetryableeErrorAutoRecover3:1");
  fault_fs_->SetFilesystemActive(true);
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();

  ASSERT_EQ("val1", Get(Key(1)));
  // Auto resume fails due to FS does not recover during resume. User call
  // resume manually here.
  s = dbfull()->Resume();
  ASSERT_EQ("val1", Get(Key(1)));
  ASSERT_EQ(s, Status::OK());
  ASSERT_OK(Put(Key(2), "val2"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ("val2", Get(Key(2)));

  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, DISABLED_FLushWritRetryableeErrorAutoRecover4) {
  // Fail the first resume and does not do resume second time because
  // the IO error severity is Fatal Error and not Retryable.
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 2;
  options.bgerror_resume_retry_interval = 10;  // 0.1 second
  Status s;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);
  IOStatus nr_msg = IOStatus::IOError("No Retryable Fatal IO Error");
  nr_msg.SetRetryable(false);

  ASSERT_OK(Put(Key(1), "val1"));
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"RecoverFromRetryableBGIOError:BeforeStart",
        "FLushWritRetryableeErrorAutoRecover4:0"},
       {"FLushWritRetryableeErrorAutoRecover4:2",
        "RecoverFromRetryableBGIOError:RecoverFail0"}});
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeFinishBuildTable",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });
  SyncPoint::GetInstance()->SetCallBack(
      "RecoverFromRetryableBGIOError:BeforeResume1",
      [&](void*) { fault_fs_->SetFilesystemActive(false, nr_msg); });

  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), CABINDB_NAMESPACE::Status::Severity::kHardError);
  TEST_SYNC_POINT("FLushWritRetryableeErrorAutoRecover4:0");
  TEST_SYNC_POINT("FLushWritRetryableeErrorAutoRecover4:2");
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  // Even the FS is recoverd, due to the Fatal Error in bg_error_ the resume
  // and flush will all fail.
  ASSERT_EQ("val1", Get(Key(1)));
  s = dbfull()->Resume();
  ASSERT_NE(s, Status::OK());
  ASSERT_EQ("val1", Get(Key(1)));
  ASSERT_OK(Put(Key(2), "val2"));
  s = Flush();
  ASSERT_NE(s, Status::OK());
  ASSERT_EQ("NOT_FOUND", Get(Key(2)));

  Reopen(options);
  ASSERT_EQ("val1", Get(Key(1)));
  ASSERT_OK(Put(Key(2), "val2"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ("val2", Get(Key(2)));

  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, DISABLED_FLushWritRetryableeErrorAutoRecover5) {
  // During the resume, call DB->CLose, make sure the resume thread exist
  // before close continues. Due to the shutdown, the resume is not successful
  // and the FS does not become active, so close status is still IO error
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 2;
  options.bgerror_resume_retry_interval = 10;  // 0.1 second
  Status s;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  ASSERT_OK(Put(Key(1), "val1"));
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"RecoverFromRetryableBGIOError:BeforeStart",
        "FLushWritRetryableeErrorAutoRecover5:0"}});
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeFinishBuildTable",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), CABINDB_NAMESPACE::Status::Severity::kHardError);
  TEST_SYNC_POINT("FLushWritRetryableeErrorAutoRecover5:0");
  // The first resume will cause recovery_error and its severity is the
  // Fatal error
  s = dbfull()->Close();
  ASSERT_NE(s, Status::OK());
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);

  Reopen(options);
  ASSERT_NE("val1", Get(Key(1)));
  ASSERT_OK(Put(Key(2), "val2"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ("val2", Get(Key(2)));

  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, FLushWritRetryableeErrorAutoRecover6) {
  // During the resume, call DB->CLose, make sure the resume thread exist
  // before close continues. Due to the shutdown, the resume is not successful
  // and the FS does not become active, so close status is still IO error
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 2;
  options.bgerror_resume_retry_interval = 10;  // 0.1 second
  Status s;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  ASSERT_OK(Put(Key(1), "val1"));
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"FLushWritRetryableeErrorAutoRecover6:0",
        "RecoverFromRetryableBGIOError:BeforeStart"},
       {"RecoverFromRetryableBGIOError:BeforeWait0",
        "FLushWritRetryableeErrorAutoRecover6:1"},
       {"FLushWritRetryableeErrorAutoRecover6:2",
        "RecoverFromRetryableBGIOError:BeforeWait1"},
       {"RecoverFromRetryableBGIOError:AfterWait0",
        "FLushWritRetryableeErrorAutoRecover6:3"}});
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeFinishBuildTable",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), CABINDB_NAMESPACE::Status::Severity::kHardError);
  TEST_SYNC_POINT("FLushWritRetryableeErrorAutoRecover6:0");
  TEST_SYNC_POINT("FLushWritRetryableeErrorAutoRecover6:1");
  fault_fs_->SetFilesystemActive(true);
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  TEST_SYNC_POINT("FLushWritRetryableeErrorAutoRecover6:2");
  TEST_SYNC_POINT("FLushWritRetryableeErrorAutoRecover6:3");
  // The first resume will cause recovery_error and its severity is the
  // Fatal error
  s = dbfull()->Close();
  ASSERT_EQ(s, Status::OK());
  SyncPoint::GetInstance()->DisableProcessing();

  Reopen(options);
  ASSERT_EQ("val1", Get(Key(1)));
  ASSERT_OK(Put(Key(2), "val2"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ("val2", Get(Key(2)));

  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, ManifestWriteRetryableErrorAutoRecover) {
  // Fail the first resume and let the second resume be successful
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 2;
  options.bgerror_resume_retry_interval = 100000;  // 0.1 second
  Status s;
  std::string old_manifest;
  std::string new_manifest;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);
  old_manifest = GetManifestNameFromLiveFiles();

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  ASSERT_OK(Put(Key(0), "val"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(Key(1), "val"));
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"RecoverFromRetryableBGIOError:BeforeStart",
        "ManifestWriteRetryableErrorAutoRecover:0"},
       {"ManifestWriteRetryableErrorAutoRecover:1",
        "RecoverFromRetryableBGIOError:BeforeWait1"},
       {"RecoverFromRetryableBGIOError:RecoverSuccess",
        "ManifestWriteRetryableErrorAutoRecover:2"}});
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), CABINDB_NAMESPACE::Status::Severity::kHardError);
  TEST_SYNC_POINT("ManifestWriteRetryableErrorAutoRecover:0");
  fault_fs_->SetFilesystemActive(true);
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  TEST_SYNC_POINT("ManifestWriteRetryableErrorAutoRecover:1");
  TEST_SYNC_POINT("ManifestWriteRetryableErrorAutoRecover:2");
  SyncPoint::GetInstance()->DisableProcessing();

  new_manifest = GetManifestNameFromLiveFiles();
  ASSERT_NE(new_manifest, old_manifest);

  Reopen(options);
  ASSERT_EQ("val", Get(Key(0)));
  ASSERT_EQ("val", Get(Key(1)));
  Close();
}

TEST_F(DBErrorHandlingFSTest,
       CompactionManifestWriteRetryableErrorAutoRecover) {
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.level0_file_num_compaction_trigger = 2;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 2;
  options.bgerror_resume_retry_interval = 100000;  // 0.1 second
  Status s;
  std::string old_manifest;
  std::string new_manifest;
  std::atomic<bool> fail_manifest(false);
  DestroyAndReopen(options);
  old_manifest = GetManifestNameFromLiveFiles();

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  ASSERT_OK(Put(Key(0), "val"));
  ASSERT_OK(Put(Key(2), "val"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());

  listener->OverrideBGError(Status(error_msg, Status::Severity::kHardError));
  listener->EnableAutoRecovery(false);
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      // Wait for flush of 2nd L0 file before starting compaction
      {{"DBImpl::FlushMemTable:FlushMemTableFinished",
        "BackgroundCallCompaction:0"},
       // Wait for compaction to detect manifest write error
       {"BackgroundCallCompaction:1", "CompactionManifestWriteErrorAR:0"},
       // Make compaction thread wait for error to be cleared
       {"CompactionManifestWriteErrorAR:1",
        "DBImpl::BackgroundCallCompaction:FoundObsoleteFiles"},
       {"CompactionManifestWriteErrorAR:2",
        "RecoverFromRetryableBGIOError:BeforeStart"},
       // Fail the first resume, before the wait in resume
       {"RecoverFromRetryableBGIOError:BeforeResume0",
        "CompactionManifestWriteErrorAR:3"},
       // Activate the FS before the second resume
       {"CompactionManifestWriteErrorAR:4",
        "RecoverFromRetryableBGIOError:BeforeResume1"},
       // Wait the auto resume be sucessful
       {"RecoverFromRetryableBGIOError:RecoverSuccess",
        "CompactionManifestWriteErrorAR:5"}});
  // trigger manifest write failure in compaction thread
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void*) { fail_manifest.store(true); });
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest", [&](void*) {
        if (fail_manifest.load()) {
          fault_fs_->SetFilesystemActive(false, error_msg);
        }
      });
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(Key(1), "val"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());

  TEST_SYNC_POINT("CompactionManifestWriteErrorAR:0");
  TEST_SYNC_POINT("CompactionManifestWriteErrorAR:1");

  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s.severity(), CABINDB_NAMESPACE::Status::Severity::kHardError);
  TEST_SYNC_POINT("CompactionManifestWriteErrorAR:2");
  TEST_SYNC_POINT("CompactionManifestWriteErrorAR:3");
  fault_fs_->SetFilesystemActive(true);
  SyncPoint::GetInstance()->ClearAllCallBacks();
  TEST_SYNC_POINT("CompactionManifestWriteErrorAR:4");
  TEST_SYNC_POINT("CompactionManifestWriteErrorAR:5");
  SyncPoint::GetInstance()->DisableProcessing();

  new_manifest = GetManifestNameFromLiveFiles();
  ASSERT_NE(new_manifest, old_manifest);

  Reopen(options);
  ASSERT_EQ("val", Get(Key(0)));
  ASSERT_EQ("val", Get(Key(1)));
  ASSERT_EQ("val", Get(Key(2)));
  Close();
}

TEST_F(DBErrorHandlingFSTest, CompactionWriteRetryableErrorAutoRecover) {
  // In this test, in the first round of compaction, the FS is set to error.
  // So the first compaction fails due to retryable IO error and it is mapped
  // to soft error. Then, compaction is rescheduled, in the second round of
  // compaction, the FS is set to active and compaction is successful, so
  // the test will hit the CompactionJob::FinishCompactionOutputFile1 sync
  // point.
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.level0_file_num_compaction_trigger = 2;
  options.listeners.emplace_back(listener);
  Status s;
  std::atomic<bool> fail_first(false);
  std::atomic<bool> fail_second(true);
  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  ASSERT_OK(Put(Key(0), "va;"));
  ASSERT_OK(Put(Key(2), "va;"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());

  listener->OverrideBGError(Status(error_msg, Status::Severity::kHardError));
  listener->EnableAutoRecovery(false);
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::FlushMemTable:FlushMemTableFinished",
        "BackgroundCallCompaction:0"},
       {"CompactionJob::FinishCompactionOutputFile1",
        "CompactionWriteRetryableErrorAutoRecover0"}});
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:Start",
      [&](void*) { fault_fs_->SetFilesystemActive(true); });
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void*) { fail_first.store(true); });
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::OpenCompactionOutputFile", [&](void*) {
        if (fail_first.load() && fail_second.load()) {
          fault_fs_->SetFilesystemActive(false, error_msg);
          fail_second.store(false);
        }
      });
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(Key(1), "val"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());

  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s.severity(), CABINDB_NAMESPACE::Status::Severity::kSoftError);

  TEST_SYNC_POINT("CompactionWriteRetryableErrorAutoRecover0");
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, WALWriteRetryableErrorAutoRecover1) {
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.writable_file_max_buffer_size = 32768;
  options.listeners.emplace_back(listener);
  options.paranoid_checks = true;
  options.max_bgerror_resume_count = 2;
  options.bgerror_resume_retry_interval = 100000;  // 0.1 second
  Status s;
  Random rnd(301);

  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  // For the first batch, write is successful, require sync
  {
    WriteBatch batch;

    for (auto i = 0; i < 100; ++i) {
      ASSERT_OK(batch.Put(Key(i), rnd.RandomString(1024)));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_EQ(dbfull()->Write(wopts, &batch), Status::OK());
  };

  // For the second batch, the first 2 file Append are successful, then the
  // following Append fails due to file system retryable IOError.
  {
    WriteBatch batch;
    int write_error = 0;

    for (auto i = 100; i < 200; ++i) {
      ASSERT_OK(batch.Put(Key(i), rnd.RandomString(1024)));
    }
    CABINDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
        {{"RecoverFromRetryableBGIOError:BeforeResume0", "WALWriteError1:0"},
         {"WALWriteError1:1", "RecoverFromRetryableBGIOError:BeforeResume1"},
         {"RecoverFromRetryableBGIOError:RecoverSuccess", "WALWriteError1:2"}});

    SyncPoint::GetInstance()->SetCallBack(
        "WritableFileWriter::Append:BeforePrepareWrite", [&](void*) {
          write_error++;
          if (write_error > 2) {
            fault_fs_->SetFilesystemActive(false, error_msg);
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
    WriteOptions wopts;
    wopts.sync = true;
    s = dbfull()->Write(wopts, &batch);
    ASSERT_EQ(true, s.IsIOError());

    TEST_SYNC_POINT("WALWriteError1:0");
    fault_fs_->SetFilesystemActive(true);
    SyncPoint::GetInstance()->ClearAllCallBacks();
    TEST_SYNC_POINT("WALWriteError1:1");
    TEST_SYNC_POINT("WALWriteError1:2");
  }
  SyncPoint::GetInstance()->DisableProcessing();

  // Data in corrupted WAL are not stored
  for (auto i = 0; i < 199; ++i) {
    if (i < 100) {
      ASSERT_NE(Get(Key(i)), "NOT_FOUND");
    } else {
      ASSERT_EQ(Get(Key(i)), "NOT_FOUND");
    }
  }

  // Resume and write a new batch, should be in the WAL
  {
    WriteBatch batch;

    for (auto i = 200; i < 300; ++i) {
      ASSERT_OK(batch.Put(Key(i), rnd.RandomString(1024)));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_EQ(dbfull()->Write(wopts, &batch), Status::OK());
  };

  Reopen(options);
  for (auto i = 0; i < 300; ++i) {
    if (i < 100 || i >= 200) {
      ASSERT_NE(Get(Key(i)), "NOT_FOUND");
    } else {
      ASSERT_EQ(Get(Key(i)), "NOT_FOUND");
    }
  }
  Close();
}

TEST_F(DBErrorHandlingFSTest, WALWriteRetryableErrorAutoRecover2) {
  // Fail the first recover and try second time.
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.writable_file_max_buffer_size = 32768;
  options.listeners.emplace_back(listener);
  options.paranoid_checks = true;
  options.max_bgerror_resume_count = 2;
  options.bgerror_resume_retry_interval = 100000;  // 0.1 second
  Status s;
  Random rnd(301);

  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  // For the first batch, write is successful, require sync
  {
    WriteBatch batch;

    for (auto i = 0; i < 100; ++i) {
      ASSERT_OK(batch.Put(Key(i), rnd.RandomString(1024)));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_EQ(dbfull()->Write(wopts, &batch), Status::OK());
  };

  // For the second batch, the first 2 file Append are successful, then the
  // following Append fails due to file system retryable IOError.
  {
    WriteBatch batch;
    int write_error = 0;

    for (auto i = 100; i < 200; ++i) {
      ASSERT_OK(batch.Put(Key(i), rnd.RandomString(1024)));
    }
    CABINDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
        {{"RecoverFromRetryableBGIOError:BeforeWait0", "WALWriteError2:0"},
         {"WALWriteError2:1", "RecoverFromRetryableBGIOError:BeforeWait1"},
         {"RecoverFromRetryableBGIOError:RecoverSuccess", "WALWriteError2:2"}});

    SyncPoint::GetInstance()->SetCallBack(
        "WritableFileWriter::Append:BeforePrepareWrite", [&](void*) {
          write_error++;
          if (write_error > 2) {
            fault_fs_->SetFilesystemActive(false, error_msg);
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
    WriteOptions wopts;
    wopts.sync = true;
    s = dbfull()->Write(wopts, &batch);
    ASSERT_EQ(true, s.IsIOError());

    TEST_SYNC_POINT("WALWriteError2:0");
    fault_fs_->SetFilesystemActive(true);
    SyncPoint::GetInstance()->ClearAllCallBacks();
    TEST_SYNC_POINT("WALWriteError2:1");
    TEST_SYNC_POINT("WALWriteError2:2");
  }
  SyncPoint::GetInstance()->DisableProcessing();

  // Data in corrupted WAL are not stored
  for (auto i = 0; i < 199; ++i) {
    if (i < 100) {
      ASSERT_NE(Get(Key(i)), "NOT_FOUND");
    } else {
      ASSERT_EQ(Get(Key(i)), "NOT_FOUND");
    }
  }

  // Resume and write a new batch, should be in the WAL
  {
    WriteBatch batch;

    for (auto i = 200; i < 300; ++i) {
      ASSERT_OK(batch.Put(Key(i), rnd.RandomString(1024)));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_EQ(dbfull()->Write(wopts, &batch), Status::OK());
  };

  Reopen(options);
  for (auto i = 0; i < 300; ++i) {
    if (i < 100 || i >= 200) {
      ASSERT_NE(Get(Key(i)), "NOT_FOUND");
    } else {
      ASSERT_EQ(Get(Key(i)), "NOT_FOUND");
    }
  }
  Close();
}

class DBErrorHandlingFencingTest : public DBErrorHandlingFSTest,
                                   public testing::WithParamInterface<bool> {};

TEST_P(DBErrorHandlingFencingTest, FLushWriteFenced) {
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.paranoid_checks = GetParam();
  Status s;

  listener->EnableAutoRecovery(true);
  DestroyAndReopen(options);

  ASSERT_OK(Put(Key(0), "val"));
  SyncPoint::GetInstance()->SetCallBack("FlushJob::Start", [&](void*) {
    fault_fs_->SetFilesystemActive(false, IOStatus::IOFenced("IO fenced"));
  });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), CABINDB_NAMESPACE::Status::Severity::kFatalError);
  ASSERT_TRUE(s.IsIOFenced());
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_TRUE(s.IsIOFenced());
  Destroy(options);
}

TEST_P(DBErrorHandlingFencingTest, ManifestWriteFenced) {
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.paranoid_checks = GetParam();
  Status s;
  std::string old_manifest;
  std::string new_manifest;

  listener->EnableAutoRecovery(true);
  DestroyAndReopen(options);
  old_manifest = GetManifestNameFromLiveFiles();

  ASSERT_OK(Put(Key(0), "val"));
  Flush();
  ASSERT_OK(Put(Key(1), "val"));
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest", [&](void*) {
        fault_fs_->SetFilesystemActive(false, IOStatus::IOFenced("IO fenced"));
      });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), CABINDB_NAMESPACE::Status::Severity::kFatalError);
  ASSERT_TRUE(s.IsIOFenced());
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_TRUE(s.IsIOFenced());
  Close();
}

TEST_P(DBErrorHandlingFencingTest, CompactionWriteFenced) {
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.level0_file_num_compaction_trigger = 2;
  options.listeners.emplace_back(listener);
  options.paranoid_checks = GetParam();
  Status s;
  DestroyAndReopen(options);

  ASSERT_OK(Put(Key(0), "va;"));
  ASSERT_OK(Put(Key(2), "va;"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());

  listener->EnableAutoRecovery(true);
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::FlushMemTable:FlushMemTableFinished",
        "BackgroundCallCompaction:0"}});
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void*) {
        fault_fs_->SetFilesystemActive(false, IOStatus::IOFenced("IO fenced"));
      });
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(Key(1), "val"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());

  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s.severity(), CABINDB_NAMESPACE::Status::Severity::kFatalError);
  ASSERT_TRUE(s.IsIOFenced());

  fault_fs_->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_TRUE(s.IsIOFenced());
  Destroy(options);
}

TEST_P(DBErrorHandlingFencingTest, WALWriteFenced) {
  std::shared_ptr<ErrorHandlerFSListener> listener(
      new ErrorHandlerFSListener());
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.writable_file_max_buffer_size = 32768;
  options.listeners.emplace_back(listener);
  options.paranoid_checks = GetParam();
  Status s;
  Random rnd(301);

  listener->EnableAutoRecovery(true);
  DestroyAndReopen(options);

  {
    WriteBatch batch;

    for (auto i = 0; i < 100; ++i) {
      ASSERT_OK(batch.Put(Key(i), rnd.RandomString(1024)));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_EQ(dbfull()->Write(wopts, &batch), Status::OK());
  };

  {
    WriteBatch batch;
    int write_error = 0;

    for (auto i = 100; i < 199; ++i) {
      ASSERT_OK(batch.Put(Key(i), rnd.RandomString(1024)));
    }

    SyncPoint::GetInstance()->SetCallBack(
        "WritableFileWriter::Append:BeforePrepareWrite", [&](void*) {
          write_error++;
          if (write_error > 2) {
            fault_fs_->SetFilesystemActive(false,
                                           IOStatus::IOFenced("IO fenced"));
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
    WriteOptions wopts;
    wopts.sync = true;
    s = dbfull()->Write(wopts, &batch);
    ASSERT_TRUE(s.IsIOFenced());
  }
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  {
    WriteBatch batch;

    for (auto i = 0; i < 100; ++i) {
      ASSERT_OK(batch.Put(Key(i), rnd.RandomString(1024)));
    }

    WriteOptions wopts;
    wopts.sync = true;
    s = dbfull()->Write(wopts, &batch);
    ASSERT_TRUE(s.IsIOFenced());
  }
  Close();
}

INSTANTIATE_TEST_CASE_P(DBErrorHandlingFSTest, DBErrorHandlingFencingTest,
                        ::testing::Bool());

}  // namespace CABINDB_NAMESPACE

int main(int argc, char** argv) {
  CABINDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr, "SKIPPED as Cuckoo table is not supported in CABINDB_LITE\n");
  return 0;
}

#endif  // CABINDB_LITE
