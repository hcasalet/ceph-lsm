//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_index.h"
#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "db/dbformat.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "file/filename.h"
#include "memtable/hash_linklist_rep.h"
#include "monitoring/statistics.h"
#include "include/cabindb/cache.h"
#include "include/cabindb/compaction_filter.h"
#include "include/cabindb/db.h"
#include "include/cabindb/env.h"
#include "include/cabindb/filter_policy.h"
#include "include/cabindb/options.h"
#include "include/cabindb/perf_context.h"
#include "include/cabindb/slice.h"
#include "include/cabindb/slice_transform.h"
#include "include/cabindb/table.h"
#include "include/cabindb/table_properties.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/hash.h"
#include "util/mutexlock.h"
#include "util/rate_limiter.h"
#include "util/string_util.h"
#include "utilities/merge_operators.h"

#ifndef CABINDB_LITE

namespace CABINDB_NAMESPACE {

class EventListenerTest : public DBTestBase {
 public:
  EventListenerTest() : DBTestBase("/listener_test", /*env_do_fsync=*/true) {}

  static std::string BlobStr(uint64_t blob_file_number, uint64_t offset,
                             uint64_t size) {
    std::string blob_index;
    BlobIndex::EncodeBlob(&blob_index, blob_file_number, offset, size,
                          kNoCompression);
    return blob_index;
  }

  const size_t k110KB = 110 << 10;
};

struct TestPropertiesCollector
    : public CABINDB_NAMESPACE::TablePropertiesCollector {
  CABINDB_NAMESPACE::Status AddUserKey(
      const CABINDB_NAMESPACE::Slice& /*key*/,
      const CABINDB_NAMESPACE::Slice& /*value*/,
      CABINDB_NAMESPACE::EntryType /*type*/,
      CABINDB_NAMESPACE::SequenceNumber /*seq*/,
      uint64_t /*file_size*/) override {
    return Status::OK();
  }
  CABINDB_NAMESPACE::Status Finish(
      CABINDB_NAMESPACE::UserCollectedProperties* properties) override {
    properties->insert({"0", "1"});
    return Status::OK();
  }

  const char* Name() const override { return "TestTablePropertiesCollector"; }

  CABINDB_NAMESPACE::UserCollectedProperties GetReadableProperties()
      const override {
    CABINDB_NAMESPACE::UserCollectedProperties ret;
    ret["2"] = "3";
    return ret;
  }
};

class TestPropertiesCollectorFactory : public TablePropertiesCollectorFactory {
 public:
  TablePropertiesCollector* CreateTablePropertiesCollector(
      TablePropertiesCollectorFactory::Context /*context*/) override {
    return new TestPropertiesCollector;
  }
  const char* Name() const override { return "TestTablePropertiesCollector"; }
};

class TestCompactionListener : public EventListener {
 public:
  explicit TestCompactionListener(EventListenerTest* test) : test_(test) {}

  void OnCompactionCompleted(DB *db, const CompactionJobInfo& ci) override {
    std::lock_guard<std::mutex> lock(mutex_);
    compacted_dbs_.push_back(db);
    ASSERT_GT(ci.input_files.size(), 0U);
    ASSERT_EQ(ci.input_files.size(), ci.input_file_infos.size());

    for (size_t i = 0; i < ci.input_file_infos.size(); ++i) {
      ASSERT_EQ(ci.input_file_infos[i].level, ci.base_input_level);
      ASSERT_EQ(ci.input_file_infos[i].file_number,
                TableFileNameToNumber(ci.input_files[i]));
    }

    ASSERT_GT(ci.output_files.size(), 0U);
    ASSERT_EQ(ci.output_files.size(), ci.output_file_infos.size());

    ASSERT_TRUE(test_);
    ASSERT_EQ(test_->db_, db);

    std::vector<std::vector<FileMetaData>> files_by_level;
    test_->dbfull()->TEST_GetFilesMetaData(test_->handles_[ci.cf_id],
                                           &files_by_level);
    ASSERT_GT(files_by_level.size(), ci.output_level);

    for (size_t i = 0; i < ci.output_file_infos.size(); ++i) {
      ASSERT_EQ(ci.output_file_infos[i].level, ci.output_level);
      ASSERT_EQ(ci.output_file_infos[i].file_number,
                TableFileNameToNumber(ci.output_files[i]));

      auto it = std::find_if(
          files_by_level[ci.output_level].begin(),
          files_by_level[ci.output_level].end(), [&](const FileMetaData& meta) {
            return meta.fd.GetNumber() == ci.output_file_infos[i].file_number;
          });
      ASSERT_NE(it, files_by_level[ci.output_level].end());

      ASSERT_EQ(ci.output_file_infos[i].oldest_blob_file_number,
                it->oldest_blob_file_number);
    }

    ASSERT_EQ(db->GetEnv()->GetThreadID(), ci.thread_id);
    ASSERT_GT(ci.thread_id, 0U);

    for (auto fl : {ci.input_files, ci.output_files}) {
      for (auto fn : fl) {
        auto it = ci.table_properties.find(fn);
        ASSERT_NE(it, ci.table_properties.end());
        auto tp = it->second;
        ASSERT_TRUE(tp != nullptr);
        ASSERT_EQ(tp->user_collected_properties.find("0")->second, "1");
      }
    }
  }

  EventListenerTest* test_;
  std::vector<DB*> compacted_dbs_;
  std::mutex mutex_;
};

TEST_F(EventListenerTest, OnSingleDBCompactionTest) {
  const int kTestKeySize = 16;
  const int kTestValueSize = 984;
  const int kEntrySize = kTestKeySize + kTestValueSize;
  const int kEntriesPerBuffer = 100;
  const int kNumL0Files = 4;

  Options options;
  options.env = CurrentOptions().env;
  options.create_if_missing = true;
  options.write_buffer_size = kEntrySize * kEntriesPerBuffer;
  options.compaction_style = kCompactionStyleLevel;
  options.target_file_size_base = options.write_buffer_size;
  options.max_bytes_for_level_base = options.target_file_size_base * 2;
  options.max_bytes_for_level_multiplier = 2;
  options.compression = kNoCompression;
#ifdef CABINDB_USING_THREAD_STATUS
  options.enable_thread_tracking = true;
#endif  // CABINDB_USING_THREAD_STATUS
  options.level0_file_num_compaction_trigger = kNumL0Files;
  options.table_properties_collector_factories.push_back(
      std::make_shared<TestPropertiesCollectorFactory>());

  TestCompactionListener* listener = new TestCompactionListener(this);
  options.listeners.emplace_back(listener);
  std::vector<std::string> cf_names = {
      "pikachu", "ilya", "muromec", "dobrynia",
      "nikitich", "alyosha", "popovich"};
  CreateAndReopenWithCF(cf_names, options);
  ASSERT_OK(Put(1, "pikachu", std::string(90000, 'p')));

  WriteBatch batch;
  ASSERT_OK(WriteBatchInternal::PutBlobIndex(&batch, 1, "ditto",
                                             BlobStr(123, 0, 1 << 10)));
  ASSERT_OK(dbfull()->Write(WriteOptions(), &batch));

  ASSERT_OK(Put(2, "ilya", std::string(90000, 'i')));
  ASSERT_OK(Put(3, "muromec", std::string(90000, 'm')));
  ASSERT_OK(Put(4, "dobrynia", std::string(90000, 'd')));
  ASSERT_OK(Put(5, "nikitich", std::string(90000, 'n')));
  ASSERT_OK(Put(6, "alyosha", std::string(90000, 'a')));
  ASSERT_OK(Put(7, "popovich", std::string(90000, 'p')));
  for (int i = 1; i < 8; ++i) {
    ASSERT_OK(Flush(i));
    dbfull()->TEST_WaitForFlushMemTable();
    ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), handles_[i],
                                     nullptr, nullptr));
    dbfull()->TEST_WaitForCompact();
  }

  ASSERT_EQ(listener->compacted_dbs_.size(), cf_names.size());
  for (size_t i = 0; i < cf_names.size(); ++i) {
    ASSERT_EQ(listener->compacted_dbs_[i], db_);
  }
}

// This simple Listener can only handle one flush at a time.
class TestFlushListener : public EventListener {
 public:
  TestFlushListener(Env* env, EventListenerTest* test)
      : slowdown_count(0), stop_count(0), db_closed(), env_(env), test_(test) {
    db_closed = false;
  }
  void OnTableFileCreated(
      const TableFileCreationInfo& info) override {
    // remember the info for later checking the FlushJobInfo.
    prev_fc_info_ = info;
    ASSERT_GT(info.db_name.size(), 0U);
    ASSERT_GT(info.cf_name.size(), 0U);
    ASSERT_GT(info.file_path.size(), 0U);
    ASSERT_GT(info.job_id, 0);
    ASSERT_GT(info.table_properties.data_size, 0U);
    ASSERT_GT(info.table_properties.raw_key_size, 0U);
    ASSERT_GT(info.table_properties.raw_value_size, 0U);
    ASSERT_GT(info.table_properties.num_data_blocks, 0U);
    ASSERT_GT(info.table_properties.num_entries, 0U);
    ASSERT_EQ(info.file_checksum, kUnknownFileChecksum);
    ASSERT_EQ(info.file_checksum_func_name, kUnknownFileChecksumFuncName);

#ifdef CABINDB_USING_THREAD_STATUS
    // Verify the id of the current thread that created this table
    // file matches the id of any active flush or compaction thread.
    uint64_t thread_id = env_->GetThreadID();
    std::vector<ThreadStatus> thread_list;
    ASSERT_OK(env_->GetThreadList(&thread_list));
    bool found_match = false;
    for (auto thread_status : thread_list) {
      if (thread_status.operation_type == ThreadStatus::OP_FLUSH ||
          thread_status.operation_type == ThreadStatus::OP_COMPACTION) {
        if (thread_id == thread_status.thread_id) {
          found_match = true;
          break;
        }
      }
    }
    ASSERT_TRUE(found_match);
#endif  // CABINDB_USING_THREAD_STATUS
  }

  void OnFlushCompleted(
      DB* db, const FlushJobInfo& info) override {
    flushed_dbs_.push_back(db);
    flushed_column_family_names_.push_back(info.cf_name);
    if (info.triggered_writes_slowdown) {
      slowdown_count++;
    }
    if (info.triggered_writes_stop) {
      stop_count++;
    }
    // verify whether the previously created file matches the flushed file.
    ASSERT_EQ(prev_fc_info_.db_name, db->GetName());
    ASSERT_EQ(prev_fc_info_.cf_name, info.cf_name);
    ASSERT_EQ(prev_fc_info_.job_id, info.job_id);
    ASSERT_EQ(prev_fc_info_.file_path, info.file_path);
    ASSERT_EQ(TableFileNameToNumber(info.file_path), info.file_number);

    // Note: the following chunk relies on the notification pertaining to the
    // database pointed to by DBTestBase::db_, and is thus bypassed when
    // that assumption does not hold (see the test case MultiDBMultiListeners
    // below).
    ASSERT_TRUE(test_);
    if (db == test_->db_) {
      std::vector<std::vector<FileMetaData>> files_by_level;
      test_->dbfull()->TEST_GetFilesMetaData(test_->handles_[info.cf_id],
                                             &files_by_level);

      ASSERT_FALSE(files_by_level.empty());
      auto it = std::find_if(files_by_level[0].begin(), files_by_level[0].end(),
                             [&](const FileMetaData& meta) {
                               return meta.fd.GetNumber() == info.file_number;
                             });
      ASSERT_NE(it, files_by_level[0].end());
      ASSERT_EQ(info.oldest_blob_file_number, it->oldest_blob_file_number);
    }

    ASSERT_EQ(db->GetEnv()->GetThreadID(), info.thread_id);
    ASSERT_GT(info.thread_id, 0U);
    ASSERT_EQ(info.table_properties.user_collected_properties.find("0")->second,
              "1");
  }

  std::vector<std::string> flushed_column_family_names_;
  std::vector<DB*> flushed_dbs_;
  int slowdown_count;
  int stop_count;
  bool db_closing;
  std::atomic_bool db_closed;
  TableFileCreationInfo prev_fc_info_;

 protected:
  Env* env_;
  EventListenerTest* test_;
};

TEST_F(EventListenerTest, OnSingleDBFlushTest) {
  Options options;
  options.env = CurrentOptions().env;
  options.write_buffer_size = k110KB;
#ifdef CABINDB_USING_THREAD_STATUS
  options.enable_thread_tracking = true;
#endif  // CABINDB_USING_THREAD_STATUS
  TestFlushListener* listener = new TestFlushListener(options.env, this);
  options.listeners.emplace_back(listener);
  std::vector<std::string> cf_names = {
      "pikachu", "ilya", "muromec", "dobrynia",
      "nikitich", "alyosha", "popovich"};
  options.table_properties_collector_factories.push_back(
      std::make_shared<TestPropertiesCollectorFactory>());
  CreateAndReopenWithCF(cf_names, options);

  ASSERT_OK(Put(1, "pikachu", std::string(90000, 'p')));

  WriteBatch batch;
  ASSERT_OK(WriteBatchInternal::PutBlobIndex(&batch, 1, "ditto",
                                             BlobStr(456, 0, 1 << 10)));
  ASSERT_OK(dbfull()->Write(WriteOptions(), &batch));

  ASSERT_OK(Put(2, "ilya", std::string(90000, 'i')));
  ASSERT_OK(Put(3, "muromec", std::string(90000, 'm')));
  ASSERT_OK(Put(4, "dobrynia", std::string(90000, 'd')));
  ASSERT_OK(Put(5, "nikitich", std::string(90000, 'n')));
  ASSERT_OK(Put(6, "alyosha", std::string(90000, 'a')));
  ASSERT_OK(Put(7, "popovich", std::string(90000, 'p')));
  for (int i = 1; i < 8; ++i) {
    ASSERT_OK(Flush(i));
    dbfull()->TEST_WaitForFlushMemTable();
    ASSERT_EQ(listener->flushed_dbs_.size(), i);
    ASSERT_EQ(listener->flushed_column_family_names_.size(), i);
  }

  // make sure callback functions are called in the right order
  for (size_t i = 0; i < cf_names.size(); ++i) {
    ASSERT_EQ(listener->flushed_dbs_[i], db_);
    ASSERT_EQ(listener->flushed_column_family_names_[i], cf_names[i]);
  }
}

TEST_F(EventListenerTest, MultiCF) {
  Options options;
  options.env = CurrentOptions().env;
  options.write_buffer_size = k110KB;
#ifdef CABINDB_USING_THREAD_STATUS
  options.enable_thread_tracking = true;
#endif  // CABINDB_USING_THREAD_STATUS
  TestFlushListener* listener = new TestFlushListener(options.env, this);
  options.listeners.emplace_back(listener);
  options.table_properties_collector_factories.push_back(
      std::make_shared<TestPropertiesCollectorFactory>());
  std::vector<std::string> cf_names = {
      "pikachu", "ilya", "muromec", "dobrynia",
      "nikitich", "alyosha", "popovich"};
  CreateAndReopenWithCF(cf_names, options);

  ASSERT_OK(Put(1, "pikachu", std::string(90000, 'p')));
  ASSERT_OK(Put(2, "ilya", std::string(90000, 'i')));
  ASSERT_OK(Put(3, "muromec", std::string(90000, 'm')));
  ASSERT_OK(Put(4, "dobrynia", std::string(90000, 'd')));
  ASSERT_OK(Put(5, "nikitich", std::string(90000, 'n')));
  ASSERT_OK(Put(6, "alyosha", std::string(90000, 'a')));
  ASSERT_OK(Put(7, "popovich", std::string(90000, 'p')));
  for (int i = 1; i < 8; ++i) {
    ASSERT_OK(Flush(i));
    ASSERT_EQ(listener->flushed_dbs_.size(), i);
    ASSERT_EQ(listener->flushed_column_family_names_.size(), i);
  }

  // make sure callback functions are called in the right order
  for (size_t i = 0; i < cf_names.size(); i++) {
    ASSERT_EQ(listener->flushed_dbs_[i], db_);
    ASSERT_EQ(listener->flushed_column_family_names_[i], cf_names[i]);
  }
}

TEST_F(EventListenerTest, MultiDBMultiListeners) {
  Options options;
  options.env = CurrentOptions().env;
#ifdef CABINDB_USING_THREAD_STATUS
  options.enable_thread_tracking = true;
#endif  // CABINDB_USING_THREAD_STATUS
  options.table_properties_collector_factories.push_back(
      std::make_shared<TestPropertiesCollectorFactory>());
  std::vector<TestFlushListener*> listeners;
  const int kNumDBs = 5;
  const int kNumListeners = 10;
  for (int i = 0; i < kNumListeners; ++i) {
    listeners.emplace_back(new TestFlushListener(options.env, this));
  }

  std::vector<std::string> cf_names = {
      "pikachu", "ilya", "muromec", "dobrynia",
      "nikitich", "alyosha", "popovich"};

  options.create_if_missing = true;
  for (int i = 0; i < kNumListeners; ++i) {
    options.listeners.emplace_back(listeners[i]);
  }
  DBOptions db_opts(options);
  ColumnFamilyOptions cf_opts(options);

  std::vector<DB*> dbs;
  std::vector<std::vector<ColumnFamilyHandle *>> vec_handles;

  for (int d = 0; d < kNumDBs; ++d) {
    ASSERT_OK(DestroyDB(dbname_ + ToString(d), options));
    DB* db;
    std::vector<ColumnFamilyHandle*> handles;
    ASSERT_OK(DB::Open(options, dbname_ + ToString(d), &db));
    for (size_t c = 0; c < cf_names.size(); ++c) {
      ColumnFamilyHandle* handle;
      db->CreateColumnFamily(cf_opts, cf_names[c], &handle);
      handles.push_back(handle);
    }

    vec_handles.push_back(std::move(handles));
    dbs.push_back(db);
  }

  for (int d = 0; d < kNumDBs; ++d) {
    for (size_t c = 0; c < cf_names.size(); ++c) {
      ASSERT_OK(dbs[d]->Put(WriteOptions(), vec_handles[d][c],
                cf_names[c], cf_names[c]));
    }
  }

  for (size_t c = 0; c < cf_names.size(); ++c) {
    for (int d = 0; d < kNumDBs; ++d) {
      ASSERT_OK(dbs[d]->Flush(FlushOptions(), vec_handles[d][c]));
      static_cast_with_check<DBImpl>(dbs[d])->TEST_WaitForFlushMemTable();
    }
  }

  for (auto* listener : listeners) {
    int pos = 0;
    for (size_t c = 0; c < cf_names.size(); ++c) {
      for (int d = 0; d < kNumDBs; ++d) {
        ASSERT_EQ(listener->flushed_dbs_[pos], dbs[d]);
        ASSERT_EQ(listener->flushed_column_family_names_[pos], cf_names[c]);
        pos++;
      }
    }
  }


  for (auto handles : vec_handles) {
    for (auto h : handles) {
      delete h;
    }
    handles.clear();
  }
  vec_handles.clear();

  for (auto db : dbs) {
    delete db;
  }
}

TEST_F(EventListenerTest, DisableBGCompaction) {
  Options options;
  options.env = CurrentOptions().env;
#ifdef CABINDB_USING_THREAD_STATUS
  options.enable_thread_tracking = true;
#endif  // CABINDB_USING_THREAD_STATUS
  TestFlushListener* listener = new TestFlushListener(options.env, this);
  const int kCompactionTrigger = 1;
  const int kSlowdownTrigger = 5;
  const int kStopTrigger = 100;
  options.level0_file_num_compaction_trigger = kCompactionTrigger;
  options.level0_slowdown_writes_trigger = kSlowdownTrigger;
  options.level0_stop_writes_trigger = kStopTrigger;
  options.max_write_buffer_number = 10;
  options.listeners.emplace_back(listener);
  // BG compaction is disabled.  Number of L0 files will simply keeps
  // increasing in this test.
  options.compaction_style = kCompactionStyleNone;
  options.compression = kNoCompression;
  options.write_buffer_size = 100000;  // Small write buffer
  options.table_properties_collector_factories.push_back(
      std::make_shared<TestPropertiesCollectorFactory>());

  CreateAndReopenWithCF({"pikachu"}, options);
  ColumnFamilyMetaData cf_meta;
  db_->GetColumnFamilyMetaData(handles_[1], &cf_meta);

  // keep writing until writes are forced to stop.
  for (int i = 0; static_cast<int>(cf_meta.file_count) < kSlowdownTrigger * 10;
       ++i) {
    Put(1, ToString(i), std::string(10000, 'x'), WriteOptions());
    FlushOptions fo;
    fo.allow_write_stall = true;
    db_->Flush(fo, handles_[1]);
    db_->GetColumnFamilyMetaData(handles_[1], &cf_meta);
  }
  ASSERT_GE(listener->slowdown_count, kSlowdownTrigger * 9);
}

class TestCompactionReasonListener : public EventListener {
 public:
  void OnCompactionCompleted(DB* /*db*/, const CompactionJobInfo& ci) override {
    std::lock_guard<std::mutex> lock(mutex_);
    compaction_reasons_.push_back(ci.compaction_reason);
  }

  std::vector<CompactionReason> compaction_reasons_;
  std::mutex mutex_;
};

TEST_F(EventListenerTest, CompactionReasonLevel) {
  Options options;
  options.env = CurrentOptions().env;
  options.create_if_missing = true;
  options.memtable_factory.reset(
      new SpecialSkipListFactory(DBTestBase::kNumKeysByGenerateNewRandomFile));

  TestCompactionReasonListener* listener = new TestCompactionReasonListener();
  options.listeners.emplace_back(listener);

  options.level0_file_num_compaction_trigger = 4;
  options.compaction_style = kCompactionStyleLevel;

  DestroyAndReopen(options);
  Random rnd(301);

  // Write 4 files in L0
  for (int i = 0; i < 4; i++) {
    GenerateNewRandomFile(&rnd);
  }
  dbfull()->TEST_WaitForCompact();

  ASSERT_EQ(listener->compaction_reasons_.size(), 1);
  ASSERT_EQ(listener->compaction_reasons_[0],
            CompactionReason::kLevelL0FilesNum);

  DestroyAndReopen(options);

  // Write 3 non-overlapping files in L0
  for (int k = 1; k <= 30; k++) {
    ASSERT_OK(Put(Key(k), Key(k)));
    if (k % 10 == 0) {
      Flush();
    }
  }

  // Do a trivial move from L0 -> L1
  db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);

  options.max_bytes_for_level_base = 1;
  Close();
  listener->compaction_reasons_.clear();
  Reopen(options);

  dbfull()->TEST_WaitForCompact();
  ASSERT_GT(listener->compaction_reasons_.size(), 1);

  for (auto compaction_reason : listener->compaction_reasons_) {
    ASSERT_EQ(compaction_reason, CompactionReason::kLevelMaxLevelSize);
  }

  options.disable_auto_compactions = true;
  Close();
  listener->compaction_reasons_.clear();
  Reopen(options);

  Put("key", "value");
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForceOptimized;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_GT(listener->compaction_reasons_.size(), 0);
  for (auto compaction_reason : listener->compaction_reasons_) {
    ASSERT_EQ(compaction_reason, CompactionReason::kManualCompaction);
  }
}

TEST_F(EventListenerTest, CompactionReasonUniversal) {
  Options options;
  options.env = CurrentOptions().env;
  options.create_if_missing = true;
  options.memtable_factory.reset(
      new SpecialSkipListFactory(DBTestBase::kNumKeysByGenerateNewRandomFile));

  TestCompactionReasonListener* listener = new TestCompactionReasonListener();
  options.listeners.emplace_back(listener);

  options.compaction_style = kCompactionStyleUniversal;

  Random rnd(301);

  options.level0_file_num_compaction_trigger = 8;
  options.compaction_options_universal.max_size_amplification_percent = 100000;
  options.compaction_options_universal.size_ratio = 100000;
  DestroyAndReopen(options);
  listener->compaction_reasons_.clear();

  // Write 8 files in L0
  for (int i = 0; i < 8; i++) {
    GenerateNewRandomFile(&rnd);
  }
  dbfull()->TEST_WaitForCompact();

  ASSERT_GT(listener->compaction_reasons_.size(), 0);
  for (auto compaction_reason : listener->compaction_reasons_) {
    ASSERT_EQ(compaction_reason, CompactionReason::kUniversalSizeRatio);
  }

  options.level0_file_num_compaction_trigger = 8;
  options.compaction_options_universal.max_size_amplification_percent = 1;
  options.compaction_options_universal.size_ratio = 100000;

  DestroyAndReopen(options);
  listener->compaction_reasons_.clear();

  // Write 8 files in L0
  for (int i = 0; i < 8; i++) {
    GenerateNewRandomFile(&rnd);
  }
  dbfull()->TEST_WaitForCompact();

  ASSERT_GT(listener->compaction_reasons_.size(), 0);
  for (auto compaction_reason : listener->compaction_reasons_) {
    ASSERT_EQ(compaction_reason, CompactionReason::kUniversalSizeAmplification);
  }

  options.disable_auto_compactions = true;
  Close();
  listener->compaction_reasons_.clear();
  Reopen(options);

  db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);

  ASSERT_GT(listener->compaction_reasons_.size(), 0);
  for (auto compaction_reason : listener->compaction_reasons_) {
    ASSERT_EQ(compaction_reason, CompactionReason::kManualCompaction);
  }
}

TEST_F(EventListenerTest, CompactionReasonFIFO) {
  Options options;
  options.env = CurrentOptions().env;
  options.create_if_missing = true;
  options.memtable_factory.reset(
      new SpecialSkipListFactory(DBTestBase::kNumKeysByGenerateNewRandomFile));

  TestCompactionReasonListener* listener = new TestCompactionReasonListener();
  options.listeners.emplace_back(listener);

  options.level0_file_num_compaction_trigger = 4;
  options.compaction_style = kCompactionStyleFIFO;
  options.compaction_options_fifo.max_table_files_size = 1;

  DestroyAndReopen(options);
  Random rnd(301);

  // Write 4 files in L0
  for (int i = 0; i < 4; i++) {
    GenerateNewRandomFile(&rnd);
  }
  dbfull()->TEST_WaitForCompact();

  ASSERT_GT(listener->compaction_reasons_.size(), 0);
  for (auto compaction_reason : listener->compaction_reasons_) {
    ASSERT_EQ(compaction_reason, CompactionReason::kFIFOMaxSize);
  }
}

class TableFileCreationListener : public EventListener {
 public:
  class TestEnv : public EnvWrapper {
   public:
    explicit TestEnv(Env* t) : EnvWrapper(t) {}

    void SetStatus(Status s) { status_ = s; }

    Status NewWritableFile(const std::string& fname,
                           std::unique_ptr<WritableFile>* result,
                           const EnvOptions& options) override {
      if (fname.size() > 4 && fname.substr(fname.size() - 4) == ".sst") {
        if (!status_.ok()) {
          return status_;
        }
      }
      return target()->NewWritableFile(fname, result, options);
    }

   private:
    Status status_;
  };

  TableFileCreationListener() {
    for (int i = 0; i < 2; i++) {
      started_[i] = finished_[i] = failure_[i] = 0;
    }
  }

  int Index(TableFileCreationReason reason) {
    int idx;
    switch (reason) {
      case TableFileCreationReason::kFlush:
        idx = 0;
        break;
      case TableFileCreationReason::kCompaction:
        idx = 1;
        break;
      default:
        idx = -1;
    }
    return idx;
  }

  void CheckAndResetCounters(int flush_started, int flush_finished,
                             int flush_failure, int compaction_started,
                             int compaction_finished, int compaction_failure) {
    ASSERT_EQ(started_[0], flush_started);
    ASSERT_EQ(finished_[0], flush_finished);
    ASSERT_EQ(failure_[0], flush_failure);
    ASSERT_EQ(started_[1], compaction_started);
    ASSERT_EQ(finished_[1], compaction_finished);
    ASSERT_EQ(failure_[1], compaction_failure);
    for (int i = 0; i < 2; i++) {
      started_[i] = finished_[i] = failure_[i] = 0;
    }
  }

  void OnTableFileCreationStarted(
      const TableFileCreationBriefInfo& info) override {
    int idx = Index(info.reason);
    if (idx >= 0) {
      started_[idx]++;
    }
    ASSERT_GT(info.db_name.size(), 0U);
    ASSERT_GT(info.cf_name.size(), 0U);
    ASSERT_GT(info.file_path.size(), 0U);
    ASSERT_GT(info.job_id, 0);
  }

  void OnTableFileCreated(const TableFileCreationInfo& info) override {
    int idx = Index(info.reason);
    if (idx >= 0) {
      finished_[idx]++;
    }
    ASSERT_GT(info.db_name.size(), 0U);
    ASSERT_GT(info.cf_name.size(), 0U);
    ASSERT_GT(info.file_path.size(), 0U);
    ASSERT_GT(info.job_id, 0);
    ASSERT_EQ(info.file_checksum, kUnknownFileChecksum);
    ASSERT_EQ(info.file_checksum_func_name, kUnknownFileChecksumFuncName);
    if (info.status.ok()) {
      ASSERT_GT(info.table_properties.data_size, 0U);
      ASSERT_GT(info.table_properties.raw_key_size, 0U);
      ASSERT_GT(info.table_properties.raw_value_size, 0U);
      ASSERT_GT(info.table_properties.num_data_blocks, 0U);
      ASSERT_GT(info.table_properties.num_entries, 0U);
    } else {
      if (idx >= 0) {
        failure_[idx]++;
      }
    }
  }

  int started_[2];
  int finished_[2];
  int failure_[2];
};

TEST_F(EventListenerTest, TableFileCreationListenersTest) {
  auto listener = std::make_shared<TableFileCreationListener>();
  Options options;
  std::unique_ptr<TableFileCreationListener::TestEnv> test_env(
      new TableFileCreationListener::TestEnv(CurrentOptions().env));
  options.create_if_missing = true;
  options.listeners.push_back(listener);
  options.env = test_env.get();
  DestroyAndReopen(options);

  ASSERT_OK(Put("foo", "aaa"));
  ASSERT_OK(Put("bar", "bbb"));
  ASSERT_OK(Flush());
  dbfull()->TEST_WaitForFlushMemTable();
  listener->CheckAndResetCounters(1, 1, 0, 0, 0, 0);
  ASSERT_OK(Put("foo", "aaa1"));
  ASSERT_OK(Put("bar", "bbb1"));
  test_env->SetStatus(Status::NotSupported("not supported"));
  ASSERT_NOK(Flush());
  listener->CheckAndResetCounters(1, 1, 1, 0, 0, 0);
  test_env->SetStatus(Status::OK());

  Reopen(options);
  ASSERT_OK(Put("foo", "aaa2"));
  ASSERT_OK(Put("bar", "bbb2"));
  ASSERT_OK(Flush());
  dbfull()->TEST_WaitForFlushMemTable();
  listener->CheckAndResetCounters(1, 1, 0, 0, 0, 0);

  const Slice kRangeStart = "a";
  const Slice kRangeEnd = "z";
  dbfull()->CompactRange(CompactRangeOptions(), &kRangeStart, &kRangeEnd);
  dbfull()->TEST_WaitForCompact();
  listener->CheckAndResetCounters(0, 0, 0, 1, 1, 0);

  ASSERT_OK(Put("foo", "aaa3"));
  ASSERT_OK(Put("bar", "bbb3"));
  ASSERT_OK(Flush());
  test_env->SetStatus(Status::NotSupported("not supported"));
  dbfull()->CompactRange(CompactRangeOptions(), &kRangeStart, &kRangeEnd);
  dbfull()->TEST_WaitForCompact();
  listener->CheckAndResetCounters(1, 1, 0, 1, 1, 1);
  Close();
}

class MemTableSealedListener : public EventListener {
private:
  SequenceNumber latest_seq_number_;
public:
  MemTableSealedListener() {}
  void OnMemTableSealed(const MemTableInfo& info) override {
    latest_seq_number_ = info.first_seqno;
  }

  void OnFlushCompleted(DB* /*db*/,
    const FlushJobInfo& flush_job_info) override {
    ASSERT_LE(flush_job_info.smallest_seqno, latest_seq_number_);
  }
};

TEST_F(EventListenerTest, MemTableSealedListenerTest) {
  auto listener = std::make_shared<MemTableSealedListener>();
  Options options;
  options.env = CurrentOptions().env;
  options.create_if_missing = true;
  options.listeners.push_back(listener);
  DestroyAndReopen(options);

  for (unsigned int i = 0; i < 10; i++) {
    std::string tag = std::to_string(i);
    ASSERT_OK(Put("foo"+tag, "aaa"));
    ASSERT_OK(Put("bar"+tag, "bbb"));

    ASSERT_OK(Flush());
  }
}

class ColumnFamilyHandleDeletionStartedListener : public EventListener {
 private:
  std::vector<std::string> cfs_;
  int counter;

 public:
  explicit ColumnFamilyHandleDeletionStartedListener(
      const std::vector<std::string>& cfs)
      : cfs_(cfs), counter(0) {
    cfs_.insert(cfs_.begin(), kDefaultColumnFamilyName);
  }
  void OnColumnFamilyHandleDeletionStarted(
      ColumnFamilyHandle* handle) override {
    ASSERT_EQ(cfs_[handle->GetID()], handle->GetName());
    counter++;
  }
  int getCounter() { return counter; }
};

TEST_F(EventListenerTest, ColumnFamilyHandleDeletionStartedListenerTest) {
  std::vector<std::string> cfs{"pikachu", "eevee", "Mewtwo"};
  auto listener =
      std::make_shared<ColumnFamilyHandleDeletionStartedListener>(cfs);
  Options options;
  options.env = CurrentOptions().env;
  options.create_if_missing = true;
  options.listeners.push_back(listener);
  CreateAndReopenWithCF(cfs, options);
  ASSERT_EQ(handles_.size(), 4);
  delete handles_[3];
  delete handles_[2];
  delete handles_[1];
  handles_.resize(1);
  ASSERT_EQ(listener->getCounter(), 3);
}

class BackgroundErrorListener : public EventListener {
 private:
  SpecialEnv* env_;
  int counter_;

 public:
  BackgroundErrorListener(SpecialEnv* env) : env_(env), counter_(0) {}

  void OnBackgroundError(BackgroundErrorReason /*reason*/,
                         Status* bg_error) override {
    if (counter_ == 0) {
      // suppress the first error and disable write-dropping such that a retry
      // can succeed.
      *bg_error = Status::OK();
      env_->drop_writes_.store(false, std::memory_order_release);
      env_->SetMockSleep(false);
    }
    ++counter_;
  }

  int counter() { return counter_; }
};

TEST_F(EventListenerTest, BackgroundErrorListenerFailedFlushTest) {
  auto listener = std::make_shared<BackgroundErrorListener>(env_);
  Options options;
  options.create_if_missing = true;
  options.env = env_;
  options.listeners.push_back(listener);
  options.memtable_factory.reset(new SpecialSkipListFactory(1));
  options.paranoid_checks = true;
  DestroyAndReopen(options);

  // the usual TEST_WaitForFlushMemTable() doesn't work for failed flushes, so
  // forge a custom one for the failed flush case.
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BGWorkFlush:done",
        "EventListenerTest:BackgroundErrorListenerFailedFlushTest:1"}});
  CABINDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  env_->drop_writes_.store(true, std::memory_order_release);
  env_->SetMockSleep();

  ASSERT_OK(Put("key0", "val"));
  ASSERT_OK(Put("key1", "val"));
  TEST_SYNC_POINT("EventListenerTest:BackgroundErrorListenerFailedFlushTest:1");
  ASSERT_EQ(1, listener->counter());
  ASSERT_OK(Put("key2", "val"));
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
}

TEST_F(EventListenerTest, BackgroundErrorListenerFailedCompactionTest) {
  auto listener = std::make_shared<BackgroundErrorListener>(env_);
  Options options;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.env = env_;
  options.level0_file_num_compaction_trigger = 2;
  options.listeners.push_back(listener);
  options.memtable_factory.reset(new SpecialSkipListFactory(2));
  options.paranoid_checks = true;
  DestroyAndReopen(options);

  // third iteration triggers the second memtable's flush
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put("key0", "val"));
    if (i > 0) {
      ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    }
    ASSERT_OK(Put("key1", "val"));
  }
  ASSERT_EQ(2, NumTableFilesAtLevel(0));

  env_->drop_writes_.store(true, std::memory_order_release);
  env_->SetMockSleep();
  ASSERT_OK(dbfull()->SetOptions({{"disable_auto_compactions", "false"}}));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(1, listener->counter());

  // trigger flush so compaction is triggered again; this time it succeeds
  // The previous failed compaction may get retried automatically, so we may
  // be left with 0 or 1 files in level 1, depending on when the retry gets
  // scheduled
  ASSERT_OK(Put("key0", "val"));
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_LE(1, NumTableFilesAtLevel(0));
}

class TestFileOperationListener : public EventListener {
 public:
  TestFileOperationListener() {
    file_reads_.store(0);
    file_reads_success_.store(0);
    file_writes_.store(0);
    file_writes_success_.store(0);
    file_flushes_.store(0);
    file_flushes_success_.store(0);
    file_closes_.store(0);
    file_closes_success_.store(0);
    file_syncs_.store(0);
    file_syncs_success_.store(0);
    file_truncates_.store(0);
    file_truncates_success_.store(0);
  }

  void OnFileReadFinish(const FileOperationInfo& info) override {
    ++file_reads_;
    if (info.status.ok()) {
      ++file_reads_success_;
    }
    ReportDuration(info);
  }

  void OnFileWriteFinish(const FileOperationInfo& info) override {
    ++file_writes_;
    if (info.status.ok()) {
      ++file_writes_success_;
    }
    ReportDuration(info);
  }

  void OnFileFlushFinish(const FileOperationInfo& info) override {
    ++file_flushes_;
    if (info.status.ok()) {
      ++file_flushes_success_;
    }
    ReportDuration(info);
  }

  void OnFileCloseFinish(const FileOperationInfo& info) override {
    ++file_closes_;
    if (info.status.ok()) {
      ++file_closes_success_;
    }
    ReportDuration(info);
  }

  void OnFileSyncFinish(const FileOperationInfo& info) override {
    ++file_syncs_;
    if (info.status.ok()) {
      ++file_syncs_success_;
    }
    ReportDuration(info);
  }

  void OnFileTruncateFinish(const FileOperationInfo& info) override {
    ++file_truncates_;
    if (info.status.ok()) {
      ++file_truncates_success_;
    }
    ReportDuration(info);
  }

  bool ShouldBeNotifiedOnFileIO() override { return true; }

  std::atomic<size_t> file_reads_;
  std::atomic<size_t> file_reads_success_;
  std::atomic<size_t> file_writes_;
  std::atomic<size_t> file_writes_success_;
  std::atomic<size_t> file_flushes_;
  std::atomic<size_t> file_flushes_success_;
  std::atomic<size_t> file_closes_;
  std::atomic<size_t> file_closes_success_;
  std::atomic<size_t> file_syncs_;
  std::atomic<size_t> file_syncs_success_;
  std::atomic<size_t> file_truncates_;
  std::atomic<size_t> file_truncates_success_;

 private:
  void ReportDuration(const FileOperationInfo& info) const {
    ASSERT_GT(info.duration.count(), 0);
  }
};

TEST_F(EventListenerTest, OnFileOperationTest) {
  Options options;
  options.env = CurrentOptions().env;
  options.create_if_missing = true;

  TestFileOperationListener* listener = new TestFileOperationListener();
  options.listeners.emplace_back(listener);

  options.use_direct_io_for_flush_and_compaction = false;
  Status s = TryReopen(options);
  if (s.IsInvalidArgument()) {
    options.use_direct_io_for_flush_and_compaction = false;
  } else {
    ASSERT_OK(s);
  }
  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "aaa"));
  dbfull()->Flush(FlushOptions());
  dbfull()->TEST_WaitForFlushMemTable();
  ASSERT_GE(listener->file_writes_.load(),
            listener->file_writes_success_.load());
  ASSERT_GT(listener->file_writes_.load(), 0);
  ASSERT_GE(listener->file_flushes_.load(),
            listener->file_flushes_success_.load());
  ASSERT_GT(listener->file_flushes_.load(), 0);
  Close();

  Reopen(options);
  ASSERT_GE(listener->file_reads_.load(), listener->file_reads_success_.load());
  ASSERT_GT(listener->file_reads_.load(), 0);
  ASSERT_GE(listener->file_closes_.load(),
            listener->file_closes_success_.load());
  ASSERT_GT(listener->file_closes_.load(), 0);
  ASSERT_GE(listener->file_syncs_.load(), listener->file_syncs_success_.load());
  ASSERT_GT(listener->file_syncs_.load(), 0);
  if (true == options.use_direct_io_for_flush_and_compaction) {
    ASSERT_GE(listener->file_truncates_.load(),
              listener->file_truncates_success_.load());
    ASSERT_GT(listener->file_truncates_.load(), 0);
  }
}

}  // namespace CABINDB_NAMESPACE

#endif  // CABINDB_LITE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
