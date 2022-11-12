//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef CABINDB_LITE

#include "include/cabindb/c.h"

#include <stdlib.h>
#include "port/port.h"
#include "include/cabindb/cache.h"
#include "include/cabindb/compaction_filter.h"
#include "include/cabindb/comparator.h"
#include "include/cabindb/convenience.h"
#include "include/cabindb/db.h"
#include "include/cabindb/env.h"
#include "include/cabindb/filter_policy.h"
#include "include/cabindb/iterator.h"
#include "include/cabindb/memtablerep.h"
#include "include/cabindb/merge_operator.h"
#include "include/cabindb/options.h"
#include "include/cabindb/rate_limiter.h"
#include "include/cabindb/slice_transform.h"
#include "include/cabindb/statistics.h"
#include "include/cabindb/status.h"
#include "include/cabindb/table.h"
#include "include/cabindb/universal_compaction.h"
#include "include/cabindb/utilities/backupable_db.h"
#include "include/cabindb/utilities/checkpoint.h"
#include "include/cabindb/utilities/db_ttl.h"
#include "include/cabindb/utilities/memory_util.h"
#include "include/cabindb/utilities/optimistic_transaction_db.h"
#include "include/cabindb/utilities/transaction.h"
#include "include/cabindb/utilities/transaction_db.h"
#include "include/cabindb/utilities/write_batch_with_index.h"
#include "include/cabindb/write_batch.h"
#include "include/cabindb/perf_context.h"
#include "utilities/merge_operators.h"

#include <vector>
#include <unordered_set>
#include <map>

using CABINDB_NAMESPACE::BackupableDBOptions;
using CABINDB_NAMESPACE::BackupEngine;
using CABINDB_NAMESPACE::BackupID;
using CABINDB_NAMESPACE::BackupInfo;
using CABINDB_NAMESPACE::BatchResult;
using CABINDB_NAMESPACE::BlockBasedTableOptions;
using CABINDB_NAMESPACE::BottommostLevelCompaction;
using CABINDB_NAMESPACE::BytewiseComparator;
using CABINDB_NAMESPACE::Cache;
using CABINDB_NAMESPACE::Checkpoint;
using CABINDB_NAMESPACE::ColumnFamilyDescriptor;
using CABINDB_NAMESPACE::ColumnFamilyHandle;
using CABINDB_NAMESPACE::ColumnFamilyOptions;
using CABINDB_NAMESPACE::CompactionFilter;
using CABINDB_NAMESPACE::CompactionFilterFactory;
using CABINDB_NAMESPACE::CompactionOptionsFIFO;
using CABINDB_NAMESPACE::CompactRangeOptions;
using CABINDB_NAMESPACE::Comparator;
using CABINDB_NAMESPACE::CompressionType;
using CABINDB_NAMESPACE::CuckooTableOptions;
using CABINDB_NAMESPACE::DB;
using CABINDB_NAMESPACE::DBOptions;
using CABINDB_NAMESPACE::DbPath;
using CABINDB_NAMESPACE::Env;
using CABINDB_NAMESPACE::EnvOptions;
using CABINDB_NAMESPACE::FileLock;
using CABINDB_NAMESPACE::FilterPolicy;
using CABINDB_NAMESPACE::FlushOptions;
using CABINDB_NAMESPACE::InfoLogLevel;
using CABINDB_NAMESPACE::IngestExternalFileOptions;
using CABINDB_NAMESPACE::Iterator;
using CABINDB_NAMESPACE::LiveFileMetaData;
using CABINDB_NAMESPACE::Logger;
using CABINDB_NAMESPACE::MemoryUtil;
using CABINDB_NAMESPACE::MergeOperator;
using CABINDB_NAMESPACE::NewBloomFilterPolicy;
using CABINDB_NAMESPACE::NewGenericRateLimiter;
using CABINDB_NAMESPACE::NewLRUCache;
using CABINDB_NAMESPACE::OptimisticTransactionDB;
using CABINDB_NAMESPACE::OptimisticTransactionOptions;
using CABINDB_NAMESPACE::Options;
using CABINDB_NAMESPACE::PerfContext;
using CABINDB_NAMESPACE::PerfLevel;
using CABINDB_NAMESPACE::PinnableSlice;
using CABINDB_NAMESPACE::RandomAccessFile;
using CABINDB_NAMESPACE::Range;
using CABINDB_NAMESPACE::RateLimiter;
using CABINDB_NAMESPACE::ReadOptions;
using CABINDB_NAMESPACE::RestoreOptions;
using CABINDB_NAMESPACE::SequentialFile;
using CABINDB_NAMESPACE::Slice;
using CABINDB_NAMESPACE::SliceParts;
using CABINDB_NAMESPACE::SliceTransform;
using CABINDB_NAMESPACE::Snapshot;
using CABINDB_NAMESPACE::SstFileWriter;
using CABINDB_NAMESPACE::Status;
using CABINDB_NAMESPACE::Transaction;
using CABINDB_NAMESPACE::TransactionDB;
using CABINDB_NAMESPACE::TransactionDBOptions;
using CABINDB_NAMESPACE::TransactionLogIterator;
using CABINDB_NAMESPACE::TransactionOptions;
using CABINDB_NAMESPACE::WALRecoveryMode;
using CABINDB_NAMESPACE::WritableFile;
using CABINDB_NAMESPACE::WriteBatch;
using CABINDB_NAMESPACE::WriteBatchWithIndex;
using CABINDB_NAMESPACE::WriteOptions;

using std::vector;
using std::unordered_set;

extern "C" {

struct cabindb_t                 { DB*               rep; };
struct cabindb_backup_engine_t   { BackupEngine*     rep; };
struct cabindb_backup_engine_info_t { std::vector<BackupInfo> rep; };
struct cabindb_restore_options_t { RestoreOptions rep; };
struct cabindb_iterator_t        { Iterator*         rep; };
struct cabindb_writebatch_t      { WriteBatch        rep; };
struct cabindb_writebatch_wi_t   { WriteBatchWithIndex* rep; };
struct cabindb_snapshot_t        { const Snapshot*   rep; };
struct cabindb_flushoptions_t    { FlushOptions      rep; };
struct cabindb_fifo_compaction_options_t { CompactionOptionsFIFO rep; };
struct cabindb_readoptions_t {
   ReadOptions rep;
   // stack variables to set pointers to in ReadOptions
   Slice upper_bound;
   Slice lower_bound;
};
struct cabindb_writeoptions_t    { WriteOptions      rep; };
struct cabindb_options_t         { Options           rep; };
struct cabindb_compactoptions_t {
  CompactRangeOptions rep;
};
struct cabindb_block_based_table_options_t  { BlockBasedTableOptions rep; };
struct cabindb_cuckoo_table_options_t  { CuckooTableOptions rep; };
struct cabindb_seqfile_t         { SequentialFile*   rep; };
struct cabindb_randomfile_t      { RandomAccessFile* rep; };
struct cabindb_writablefile_t    { WritableFile*     rep; };
struct cabindb_wal_iterator_t { TransactionLogIterator* rep; };
struct cabindb_wal_readoptions_t { TransactionLogIterator::ReadOptions rep; };
struct cabindb_filelock_t        { FileLock*         rep; };
struct cabindb_logger_t {
  std::shared_ptr<Logger> rep;
};
struct cabindb_cache_t {
  std::shared_ptr<Cache> rep;
};
struct cabindb_livefiles_t       { std::vector<LiveFileMetaData> rep; };
struct cabindb_column_family_handle_t  { ColumnFamilyHandle* rep; };
struct cabindb_envoptions_t      { EnvOptions        rep; };
struct cabindb_ingestexternalfileoptions_t  { IngestExternalFileOptions rep; };
struct cabindb_sstfilewriter_t   { SstFileWriter*    rep; };
struct cabindb_ratelimiter_t {
  std::shared_ptr<RateLimiter> rep;
};
struct cabindb_perfcontext_t     { PerfContext*      rep; };
struct cabindb_pinnableslice_t {
  PinnableSlice rep;
};
struct cabindb_transactiondb_options_t {
  TransactionDBOptions rep;
};
struct cabindb_transactiondb_t {
  TransactionDB* rep;
};
struct cabindb_transaction_options_t {
  TransactionOptions rep;
};
struct cabindb_transaction_t {
  Transaction* rep;
};
struct cabindb_backupable_db_options_t {
  BackupableDBOptions rep;
};
struct cabindb_checkpoint_t {
  Checkpoint* rep;
};
struct cabindb_optimistictransactiondb_t {
  OptimisticTransactionDB* rep;
};
struct cabindb_optimistictransaction_options_t {
  OptimisticTransactionOptions rep;
};

struct cabindb_compactionfiltercontext_t {
  CompactionFilter::Context rep;
};

struct cabindb_compactionfilter_t : public CompactionFilter {
  void* state_;
  void (*destructor_)(void*);
  unsigned char (*filter_)(
      void*,
      int level,
      const char* key, size_t key_length,
      const char* existing_value, size_t value_length,
      char** new_value, size_t *new_value_length,
      unsigned char* value_changed);
  const char* (*name_)(void*);
  unsigned char ignore_snapshots_;

  ~cabindb_compactionfilter_t() override { (*destructor_)(state_); }

  bool Filter(int level, const Slice& key, const Slice& existing_value,
              std::string* new_value, bool* value_changed) const override {
    char* c_new_value = nullptr;
    size_t new_value_length = 0;
    unsigned char c_value_changed = 0;
    unsigned char result = (*filter_)(
        state_,
        level,
        key.data(), key.size(),
        existing_value.data(), existing_value.size(),
        &c_new_value, &new_value_length, &c_value_changed);
    if (c_value_changed) {
      new_value->assign(c_new_value, new_value_length);
      *value_changed = true;
    }
    return result;
  }

  const char* Name() const override { return (*name_)(state_); }

  bool IgnoreSnapshots() const override { return ignore_snapshots_; }
};

struct cabindb_compactionfilterfactory_t : public CompactionFilterFactory {
  void* state_;
  void (*destructor_)(void*);
  cabindb_compactionfilter_t* (*create_compaction_filter_)(
      void*, cabindb_compactionfiltercontext_t* context);
  const char* (*name_)(void*);

  ~cabindb_compactionfilterfactory_t() override { (*destructor_)(state_); }

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) override {
    cabindb_compactionfiltercontext_t ccontext;
    ccontext.rep = context;
    CompactionFilter* cf = (*create_compaction_filter_)(state_, &ccontext);
    return std::unique_ptr<CompactionFilter>(cf);
  }

  const char* Name() const override { return (*name_)(state_); }
};

struct cabindb_comparator_t : public Comparator {
  void* state_;
  void (*destructor_)(void*);
  int (*compare_)(
      void*,
      const char* a, size_t alen,
      const char* b, size_t blen);
  const char* (*name_)(void*);

  ~cabindb_comparator_t() override { (*destructor_)(state_); }

  int Compare(const Slice& a, const Slice& b) const override {
    return (*compare_)(state_, a.data(), a.size(), b.data(), b.size());
  }

  const char* Name() const override { return (*name_)(state_); }

  // No-ops since the C binding does not support key shortening methods.
  void FindShortestSeparator(std::string*, const Slice&) const override {}
  void FindShortSuccessor(std::string* /*key*/) const override {}
};

struct cabindb_filterpolicy_t : public FilterPolicy {
  void* state_;
  void (*destructor_)(void*);
  const char* (*name_)(void*);
  char* (*create_)(
      void*,
      const char* const* key_array, const size_t* key_length_array,
      int num_keys,
      size_t* filter_length);
  unsigned char (*key_match_)(
      void*,
      const char* key, size_t length,
      const char* filter, size_t filter_length);
  void (*delete_filter_)(
      void*,
      const char* filter, size_t filter_length);

  ~cabindb_filterpolicy_t() override { (*destructor_)(state_); }

  const char* Name() const override { return (*name_)(state_); }

  void CreateFilter(const Slice* keys, int n, std::string* dst) const override {
    std::vector<const char*> key_pointers(n);
    std::vector<size_t> key_sizes(n);
    for (int i = 0; i < n; i++) {
      key_pointers[i] = keys[i].data();
      key_sizes[i] = keys[i].size();
    }
    size_t len;
    char* filter = (*create_)(state_, &key_pointers[0], &key_sizes[0], n, &len);
    dst->append(filter, len);

    if (delete_filter_ != nullptr) {
      (*delete_filter_)(state_, filter, len);
    } else {
      free(filter);
    }
  }

  bool KeyMayMatch(const Slice& key, const Slice& filter) const override {
    return (*key_match_)(state_, key.data(), key.size(),
                         filter.data(), filter.size());
  }
};

struct cabindb_mergeoperator_t : public MergeOperator {
  void* state_;
  void (*destructor_)(void*);
  const char* (*name_)(void*);
  char* (*full_merge_)(
      void*,
      const char* key, size_t key_length,
      const char* existing_value, size_t existing_value_length,
      const char* const* operands_list, const size_t* operands_list_length,
      int num_operands,
      unsigned char* success, size_t* new_value_length);
  char* (*partial_merge_)(void*, const char* key, size_t key_length,
                          const char* const* operands_list,
                          const size_t* operands_list_length, int num_operands,
                          unsigned char* success, size_t* new_value_length);
  void (*delete_value_)(
      void*,
      const char* value, size_t value_length);

  ~cabindb_mergeoperator_t() override { (*destructor_)(state_); }

  const char* Name() const override { return (*name_)(state_); }

  bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override {
    size_t n = merge_in.operand_list.size();
    std::vector<const char*> operand_pointers(n);
    std::vector<size_t> operand_sizes(n);
    for (size_t i = 0; i < n; i++) {
      Slice operand(merge_in.operand_list[i]);
      operand_pointers[i] = operand.data();
      operand_sizes[i] = operand.size();
    }

    const char* existing_value_data = nullptr;
    size_t existing_value_len = 0;
    if (merge_in.existing_value != nullptr) {
      existing_value_data = merge_in.existing_value->data();
      existing_value_len = merge_in.existing_value->size();
    }

    unsigned char success;
    size_t new_value_len;
    char* tmp_new_value = (*full_merge_)(
        state_, merge_in.key.data(), merge_in.key.size(), existing_value_data,
        existing_value_len, &operand_pointers[0], &operand_sizes[0],
        static_cast<int>(n), &success, &new_value_len);
    merge_out->new_value.assign(tmp_new_value, new_value_len);

    if (delete_value_ != nullptr) {
      (*delete_value_)(state_, tmp_new_value, new_value_len);
    } else {
      free(tmp_new_value);
    }

    return success;
  }

  bool PartialMergeMulti(const Slice& key,
                         const std::deque<Slice>& operand_list,
                         std::string* new_value,
                         Logger* /*logger*/) const override {
    size_t operand_count = operand_list.size();
    std::vector<const char*> operand_pointers(operand_count);
    std::vector<size_t> operand_sizes(operand_count);
    for (size_t i = 0; i < operand_count; ++i) {
      Slice operand(operand_list[i]);
      operand_pointers[i] = operand.data();
      operand_sizes[i] = operand.size();
    }

    unsigned char success;
    size_t new_value_len;
    char* tmp_new_value = (*partial_merge_)(
        state_, key.data(), key.size(), &operand_pointers[0], &operand_sizes[0],
        static_cast<int>(operand_count), &success, &new_value_len);
    new_value->assign(tmp_new_value, new_value_len);

    if (delete_value_ != nullptr) {
      (*delete_value_)(state_, tmp_new_value, new_value_len);
    } else {
      free(tmp_new_value);
    }

    return success;
  }
};

struct cabindb_dbpath_t {
  DbPath rep;
};

struct cabindb_env_t {
  Env* rep;
  bool is_default;
};

struct cabindb_slicetransform_t : public SliceTransform {
  void* state_;
  void (*destructor_)(void*);
  const char* (*name_)(void*);
  char* (*transform_)(
      void*,
      const char* key, size_t length,
      size_t* dst_length);
  unsigned char (*in_domain_)(
      void*,
      const char* key, size_t length);
  unsigned char (*in_range_)(
      void*,
      const char* key, size_t length);

  ~cabindb_slicetransform_t() override { (*destructor_)(state_); }

  const char* Name() const override { return (*name_)(state_); }

  Slice Transform(const Slice& src) const override {
    size_t len;
    char* dst = (*transform_)(state_, src.data(), src.size(), &len);
    return Slice(dst, len);
  }

  bool InDomain(const Slice& src) const override {
    return (*in_domain_)(state_, src.data(), src.size());
  }

  bool InRange(const Slice& src) const override {
    return (*in_range_)(state_, src.data(), src.size());
  }
};

struct cabindb_universal_compaction_options_t {
  CABINDB_NAMESPACE::CompactionOptionsUniversal* rep;
};

static bool SaveError(char** errptr, const Status& s) {
  assert(errptr != nullptr);
  if (s.ok()) {
    return false;
  } else if (*errptr == nullptr) {
    *errptr = strdup(s.ToString().c_str());
  } else {
    // TODO(sanjay): Merge with existing error?
    // This is a bug if *errptr is not created by malloc()
    free(*errptr);
    *errptr = strdup(s.ToString().c_str());
  }
  return true;
}

static char* CopyString(const std::string& str) {
  char* result = reinterpret_cast<char*>(malloc(sizeof(char) * str.size()));
  memcpy(result, str.data(), sizeof(char) * str.size());
  return result;
}

cabindb_t* cabindb_open(
    const cabindb_options_t* options,
    const char* name,
    char** errptr) {
  DB* db;
  if (SaveError(errptr, DB::Open(options->rep, std::string(name), &db))) {
    return nullptr;
  }
  cabindb_t* result = new cabindb_t;
  result->rep = db;
  return result;
}

cabindb_t* cabindb_open_with_ttl(
    const cabindb_options_t* options,
    const char* name,
    int ttl,
    char** errptr) {
  CABINDB_NAMESPACE::DBWithTTL* db;
  if (SaveError(errptr, CABINDB_NAMESPACE::DBWithTTL::Open(
                            options->rep, std::string(name), &db, ttl))) {
    return nullptr;
  }
  cabindb_t* result = new cabindb_t;
  result->rep = db;
  return result;
}

cabindb_t* cabindb_open_for_read_only(const cabindb_options_t* options,
                                      const char* name,
                                      unsigned char error_if_wal_file_exists,
                                      char** errptr) {
  DB* db;
  if (SaveError(errptr, DB::OpenForReadOnly(options->rep, std::string(name),
                                            &db, error_if_wal_file_exists))) {
    return nullptr;
  }
  cabindb_t* result = new cabindb_t;
  result->rep = db;
  return result;
}

cabindb_t* cabindb_open_as_secondary(const cabindb_options_t* options,
                                     const char* name,
                                     const char* secondary_path,
                                     char** errptr) {
  DB* db;
  if (SaveError(errptr,
                DB::OpenAsSecondary(options->rep, std::string(name),
                                    std::string(secondary_path), &db))) {
    return nullptr;
  }
  cabindb_t* result = new cabindb_t;
  result->rep = db;
  return result;
}

cabindb_backup_engine_t* cabindb_backup_engine_open(
    const cabindb_options_t* options, const char* path, char** errptr) {
  BackupEngine* be;
  if (SaveError(errptr, BackupEngine::Open(options->rep.env,
                                           BackupableDBOptions(path,
                                                               nullptr,
                                                               true,
                                                               options->rep.info_log.get()),
                                           &be))) {
    return nullptr;
  }
  cabindb_backup_engine_t* result = new cabindb_backup_engine_t;
  result->rep = be;
  return result;
}

cabindb_backup_engine_t* cabindb_backup_engine_open_opts(
    const cabindb_backupable_db_options_t* options, cabindb_env_t* env,
    char** errptr) {
  BackupEngine* be;
  if (SaveError(errptr, BackupEngine::Open(options->rep, env->rep, &be))) {
    return nullptr;
  }
  cabindb_backup_engine_t* result = new cabindb_backup_engine_t;
  result->rep = be;
  return result;
}

void cabindb_backup_engine_create_new_backup(cabindb_backup_engine_t* be,
                                             cabindb_t* db,
                                             char** errptr) {
  SaveError(errptr, be->rep->CreateNewBackup(db->rep));
}

void cabindb_backup_engine_create_new_backup_flush(cabindb_backup_engine_t* be,
                                                   cabindb_t* db,
                                                   unsigned char flush_before_backup,
                                                   char** errptr) {
  SaveError(errptr, be->rep->CreateNewBackup(db->rep, flush_before_backup));
}

void cabindb_backup_engine_purge_old_backups(cabindb_backup_engine_t* be,
                                             uint32_t num_backups_to_keep,
                                             char** errptr) {
  SaveError(errptr, be->rep->PurgeOldBackups(num_backups_to_keep));
}

cabindb_restore_options_t* cabindb_restore_options_create() {
  return new cabindb_restore_options_t;
}

void cabindb_restore_options_destroy(cabindb_restore_options_t* opt) {
  delete opt;
}

void cabindb_restore_options_set_keep_log_files(cabindb_restore_options_t* opt,
                                                int v) {
  opt->rep.keep_log_files = v;
}


void cabindb_backup_engine_verify_backup(cabindb_backup_engine_t* be,
    uint32_t backup_id, char** errptr) {
  SaveError(errptr, be->rep->VerifyBackup(static_cast<BackupID>(backup_id)));
}

void cabindb_backup_engine_restore_db_from_latest_backup(
    cabindb_backup_engine_t* be, const char* db_dir, const char* wal_dir,
    const cabindb_restore_options_t* restore_options, char** errptr) {
  SaveError(errptr, be->rep->RestoreDBFromLatestBackup(std::string(db_dir),
                                                       std::string(wal_dir),
                                                       restore_options->rep));
}

void cabindb_backup_engine_restore_db_from_backup(
    cabindb_backup_engine_t* be, const char* db_dir, const char* wal_dir,
    const cabindb_restore_options_t* restore_options, const uint32_t backup_id,
    char** errptr) {
  SaveError(errptr, be->rep->RestoreDBFromBackup(backup_id, std::string(db_dir),
                                                 std::string(wal_dir),
                                                 restore_options->rep));
}

const cabindb_backup_engine_info_t* cabindb_backup_engine_get_backup_info(
    cabindb_backup_engine_t* be) {
  cabindb_backup_engine_info_t* result = new cabindb_backup_engine_info_t;
  be->rep->GetBackupInfo(&result->rep);
  return result;
}

int cabindb_backup_engine_info_count(const cabindb_backup_engine_info_t* info) {
  return static_cast<int>(info->rep.size());
}

int64_t cabindb_backup_engine_info_timestamp(
    const cabindb_backup_engine_info_t* info, int index) {
  return info->rep[index].timestamp;
}

uint32_t cabindb_backup_engine_info_backup_id(
    const cabindb_backup_engine_info_t* info, int index) {
  return info->rep[index].backup_id;
}

uint64_t cabindb_backup_engine_info_size(
    const cabindb_backup_engine_info_t* info, int index) {
  return info->rep[index].size;
}

uint32_t cabindb_backup_engine_info_number_files(
    const cabindb_backup_engine_info_t* info, int index) {
  return info->rep[index].number_files;
}

void cabindb_backup_engine_info_destroy(
    const cabindb_backup_engine_info_t* info) {
  delete info;
}

void cabindb_backup_engine_close(cabindb_backup_engine_t* be) {
  delete be->rep;
  delete be;
}

cabindb_backupable_db_options_t* cabindb_backupable_db_options_create(
    const char* backup_dir) {
  return new cabindb_backupable_db_options_t{
      BackupableDBOptions(std::string(backup_dir))};
}

void cabindb_backupable_db_options_set_backup_dir(
    cabindb_backupable_db_options_t* options, const char* backup_dir) {
  options->rep.backup_dir = std::string(backup_dir);
}

void cabindb_backupable_db_options_set_env(
    cabindb_backupable_db_options_t* options, cabindb_env_t* env) {
  options->rep.backup_env = (env ? env->rep : nullptr);
}

void cabindb_backupable_db_options_set_share_table_files(
    cabindb_backupable_db_options_t* options, unsigned char val) {
  options->rep.share_table_files = val;
}

unsigned char cabindb_backupable_db_options_get_share_table_files(
    cabindb_backupable_db_options_t* options) {
  return options->rep.share_table_files;
}

void cabindb_backupable_db_options_set_sync(
    cabindb_backupable_db_options_t* options, unsigned char val) {
  options->rep.sync = val;
}

unsigned char cabindb_backupable_db_options_get_sync(
    cabindb_backupable_db_options_t* options) {
  return options->rep.sync;
}

void cabindb_backupable_db_options_set_destroy_old_data(
    cabindb_backupable_db_options_t* options, unsigned char val) {
  options->rep.destroy_old_data = val;
}

unsigned char cabindb_backupable_db_options_get_destroy_old_data(
    cabindb_backupable_db_options_t* options) {
  return options->rep.destroy_old_data;
}

void cabindb_backupable_db_options_set_backup_log_files(
    cabindb_backupable_db_options_t* options, unsigned char val) {
  options->rep.backup_log_files = val;
}

unsigned char cabindb_backupable_db_options_get_backup_log_files(
    cabindb_backupable_db_options_t* options) {
  return options->rep.backup_log_files;
}

void cabindb_backupable_db_options_set_backup_rate_limit(
    cabindb_backupable_db_options_t* options, uint64_t limit) {
  options->rep.backup_rate_limit = limit;
}

uint64_t cabindb_backupable_db_options_get_backup_rate_limit(
    cabindb_backupable_db_options_t* options) {
  return options->rep.backup_rate_limit;
}

void cabindb_backupable_db_options_set_restore_rate_limit(
    cabindb_backupable_db_options_t* options, uint64_t limit) {
  options->rep.restore_rate_limit = limit;
}

uint64_t cabindb_backupable_db_options_get_restore_rate_limit(
    cabindb_backupable_db_options_t* options) {
  return options->rep.restore_rate_limit;
}

void cabindb_backupable_db_options_set_max_background_operations(
    cabindb_backupable_db_options_t* options, int val) {
  options->rep.max_background_operations = val;
}

int cabindb_backupable_db_options_get_max_background_operations(
    cabindb_backupable_db_options_t* options) {
  return options->rep.max_background_operations;
}

void cabindb_backupable_db_options_set_callback_trigger_interval_size(
    cabindb_backupable_db_options_t* options, uint64_t size) {
  options->rep.callback_trigger_interval_size = size;
}

uint64_t cabindb_backupable_db_options_get_callback_trigger_interval_size(
    cabindb_backupable_db_options_t* options) {
  return options->rep.callback_trigger_interval_size;
}

void cabindb_backupable_db_options_set_max_valid_backups_to_open(
    cabindb_backupable_db_options_t* options, int val) {
  options->rep.max_valid_backups_to_open = val;
}

int cabindb_backupable_db_options_get_max_valid_backups_to_open(
    cabindb_backupable_db_options_t* options) {
  return options->rep.max_valid_backups_to_open;
}

void cabindb_backupable_db_options_set_share_files_with_checksum_naming(
    cabindb_backupable_db_options_t* options, int val) {
  options->rep.share_files_with_checksum_naming =
      static_cast<BackupableDBOptions::ShareFilesNaming>(val);
}

int cabindb_backupable_db_options_get_share_files_with_checksum_naming(
    cabindb_backupable_db_options_t* options) {
  return static_cast<int>(options->rep.share_files_with_checksum_naming);
}

void cabindb_backupable_db_options_destroy(
    cabindb_backupable_db_options_t* options) {
  delete options;
}

cabindb_checkpoint_t* cabindb_checkpoint_object_create(cabindb_t* db,
                                                       char** errptr) {
  Checkpoint* checkpoint;
  if (SaveError(errptr, Checkpoint::Create(db->rep, &checkpoint))) {
    return nullptr;
  }
  cabindb_checkpoint_t* result = new cabindb_checkpoint_t;
  result->rep = checkpoint;
  return result;
}

void cabindb_checkpoint_create(cabindb_checkpoint_t* checkpoint,
                               const char* checkpoint_dir,
                               uint64_t log_size_for_flush, char** errptr) {
  SaveError(errptr, checkpoint->rep->CreateCheckpoint(
                        std::string(checkpoint_dir), log_size_for_flush));
}

void cabindb_checkpoint_object_destroy(cabindb_checkpoint_t* checkpoint) {
  delete checkpoint->rep;
  delete checkpoint;
}

void cabindb_close(cabindb_t* db) {
  delete db->rep;
  delete db;
}

void cabindb_options_set_uint64add_merge_operator(cabindb_options_t* opt) {
  opt->rep.merge_operator =
      CABINDB_NAMESPACE::MergeOperators::CreateUInt64AddOperator();
}

cabindb_t* cabindb_open_column_families(
    const cabindb_options_t* db_options, const char* name,
    int num_column_families, const char* const* column_family_names,
    const cabindb_options_t* const* column_family_options,
    cabindb_column_family_handle_t** column_family_handles, char** errptr) {
  std::vector<ColumnFamilyDescriptor> column_families;
  for (int i = 0; i < num_column_families; i++) {
    column_families.push_back(ColumnFamilyDescriptor(
        std::string(column_family_names[i]),
        ColumnFamilyOptions(column_family_options[i]->rep)));
  }

  DB* db;
  std::vector<ColumnFamilyHandle*> handles;
  if (SaveError(errptr, DB::Open(DBOptions(db_options->rep),
          std::string(name), column_families, &handles, &db))) {
    return nullptr;
  }

  for (size_t i = 0; i < handles.size(); i++) {
    cabindb_column_family_handle_t* c_handle = new cabindb_column_family_handle_t;
    c_handle->rep = handles[i];
    column_family_handles[i] = c_handle;
  }
  cabindb_t* result = new cabindb_t;
  result->rep = db;
  return result;
}

cabindb_t* cabindb_open_column_families_with_ttl(
    const cabindb_options_t* db_options, const char* name,
    int num_column_families, const char* const* column_family_names,
    const cabindb_options_t* const* column_family_options,
    cabindb_column_family_handle_t** column_family_handles, const int* ttls,
    char** errptr) {
  std::vector<int32_t> ttls_vec;
  std::vector<ColumnFamilyDescriptor> column_families;
  for (int i = 0; i < num_column_families; i++) {
    ttls_vec.push_back(ttls[i]);

    column_families.push_back(ColumnFamilyDescriptor(
        std::string(column_family_names[i]),
        ColumnFamilyOptions(column_family_options[i]->rep)));
  }

  CABINDB_NAMESPACE::DBWithTTL* db;
  std::vector<ColumnFamilyHandle*> handles;
  if (SaveError(errptr, CABINDB_NAMESPACE::DBWithTTL::Open(
                            DBOptions(db_options->rep), std::string(name),
                            column_families, &handles, &db, ttls_vec))) {
    return nullptr;
  }

  for (size_t i = 0; i < handles.size(); i++) {
    cabindb_column_family_handle_t* c_handle =
        new cabindb_column_family_handle_t;
    c_handle->rep = handles[i];
    column_family_handles[i] = c_handle;
  }
  cabindb_t* result = new cabindb_t;
  result->rep = db;
  return result;
}

cabindb_t* cabindb_open_for_read_only_column_families(
    const cabindb_options_t* db_options, const char* name,
    int num_column_families, const char* const* column_family_names,
    const cabindb_options_t* const* column_family_options,
    cabindb_column_family_handle_t** column_family_handles,
    unsigned char error_if_wal_file_exists, char** errptr) {
  std::vector<ColumnFamilyDescriptor> column_families;
  for (int i = 0; i < num_column_families; i++) {
    column_families.push_back(ColumnFamilyDescriptor(
        std::string(column_family_names[i]),
        ColumnFamilyOptions(column_family_options[i]->rep)));
  }

  DB* db;
  std::vector<ColumnFamilyHandle*> handles;
  if (SaveError(errptr,
                DB::OpenForReadOnly(DBOptions(db_options->rep),
                                    std::string(name), column_families,
                                    &handles, &db, error_if_wal_file_exists))) {
    return nullptr;
  }

  for (size_t i = 0; i < handles.size(); i++) {
    cabindb_column_family_handle_t* c_handle = new cabindb_column_family_handle_t;
    c_handle->rep = handles[i];
    column_family_handles[i] = c_handle;
  }
  cabindb_t* result = new cabindb_t;
  result->rep = db;
  return result;
}

cabindb_t* cabindb_open_as_secondary_column_families(
    const cabindb_options_t* db_options, const char* name,
    const char* secondary_path, int num_column_families,
    const char* const* column_family_names,
    const cabindb_options_t* const* column_family_options,
    cabindb_column_family_handle_t** column_family_handles, char** errptr) {
  std::vector<ColumnFamilyDescriptor> column_families;
  for (int i = 0; i != num_column_families; ++i) {
    column_families.emplace_back(
        std::string(column_family_names[i]),
        ColumnFamilyOptions(column_family_options[i]->rep));
  }
  DB* db;
  std::vector<ColumnFamilyHandle*> handles;
  if (SaveError(errptr, DB::OpenAsSecondary(DBOptions(db_options->rep),
                                            std::string(name),
                                            std::string(secondary_path),
                                            column_families, &handles, &db))) {
    return nullptr;
  }
  for (size_t i = 0; i != handles.size(); ++i) {
    cabindb_column_family_handle_t* c_handle =
        new cabindb_column_family_handle_t;
    c_handle->rep = handles[i];
    column_family_handles[i] = c_handle;
  }
  cabindb_t* result = new cabindb_t;
  result->rep = db;
  return result;
}

char** cabindb_list_column_families(
    const cabindb_options_t* options,
    const char* name,
    size_t* lencfs,
    char** errptr) {
  std::vector<std::string> fams;
  SaveError(errptr,
      DB::ListColumnFamilies(DBOptions(options->rep),
        std::string(name), &fams));

  *lencfs = fams.size();
  char** column_families = static_cast<char**>(malloc(sizeof(char*) * fams.size()));
  for (size_t i = 0; i < fams.size(); i++) {
    column_families[i] = strdup(fams[i].c_str());
  }
  return column_families;
}

void cabindb_list_column_families_destroy(char** list, size_t len) {
  for (size_t i = 0; i < len; ++i) {
    free(list[i]);
  }
  free(list);
}

cabindb_column_family_handle_t* cabindb_create_column_family(
    cabindb_t* db,
    const cabindb_options_t* column_family_options,
    const char* column_family_name,
    char** errptr) {
  cabindb_column_family_handle_t* handle = new cabindb_column_family_handle_t;
  SaveError(errptr,
      db->rep->CreateColumnFamily(ColumnFamilyOptions(column_family_options->rep),
        std::string(column_family_name), &(handle->rep)));
  return handle;
}

cabindb_column_family_handle_t* cabindb_create_column_family_with_ttl(
    cabindb_t* db, const cabindb_options_t* column_family_options,
    const char* column_family_name, int ttl, char** errptr) {
  CABINDB_NAMESPACE::DBWithTTL* db_with_ttl =
      static_cast<CABINDB_NAMESPACE::DBWithTTL*>(db->rep);
  cabindb_column_family_handle_t* handle = new cabindb_column_family_handle_t;
  SaveError(errptr, db_with_ttl->CreateColumnFamilyWithTtl(
                        ColumnFamilyOptions(column_family_options->rep),
                        std::string(column_family_name), &(handle->rep), ttl));
  return handle;
}

void cabindb_drop_column_family(
    cabindb_t* db,
    cabindb_column_family_handle_t* handle,
    char** errptr) {
  SaveError(errptr, db->rep->DropColumnFamily(handle->rep));
}

void cabindb_column_family_handle_destroy(cabindb_column_family_handle_t* handle) {
  delete handle->rep;
  delete handle;
}

void cabindb_put(
    cabindb_t* db,
    const cabindb_writeoptions_t* options,
    const char* key, size_t keylen,
    const char* val, size_t vallen,
    char** errptr) {
  SaveError(errptr,
            db->rep->Put(options->rep, Slice(key, keylen), Slice(val, vallen)));
}

void cabindb_put_cf(
    cabindb_t* db,
    const cabindb_writeoptions_t* options,
    cabindb_column_family_handle_t* column_family,
    const char* key, size_t keylen,
    const char* val, size_t vallen,
    char** errptr) {
  SaveError(errptr,
            db->rep->Put(options->rep, column_family->rep,
              Slice(key, keylen), Slice(val, vallen)));
}

void cabindb_delete(
    cabindb_t* db,
    const cabindb_writeoptions_t* options,
    const char* key, size_t keylen,
    char** errptr) {
  SaveError(errptr, db->rep->Delete(options->rep, Slice(key, keylen)));
}

void cabindb_delete_cf(
    cabindb_t* db,
    const cabindb_writeoptions_t* options,
    cabindb_column_family_handle_t* column_family,
    const char* key, size_t keylen,
    char** errptr) {
  SaveError(errptr, db->rep->Delete(options->rep, column_family->rep,
        Slice(key, keylen)));
}

void cabindb_delete_range_cf(cabindb_t* db,
                             const cabindb_writeoptions_t* options,
                             cabindb_column_family_handle_t* column_family,
                             const char* start_key, size_t start_key_len,
                             const char* end_key, size_t end_key_len,
                             char** errptr) {
  SaveError(errptr, db->rep->DeleteRange(options->rep, column_family->rep,
                                         Slice(start_key, start_key_len),
                                         Slice(end_key, end_key_len)));
}

void cabindb_merge(
    cabindb_t* db,
    const cabindb_writeoptions_t* options,
    const char* key, size_t keylen,
    const char* val, size_t vallen,
    char** errptr) {
  SaveError(errptr,
            db->rep->Merge(options->rep, Slice(key, keylen), Slice(val, vallen)));
}

void cabindb_merge_cf(
    cabindb_t* db,
    const cabindb_writeoptions_t* options,
    cabindb_column_family_handle_t* column_family,
    const char* key, size_t keylen,
    const char* val, size_t vallen,
    char** errptr) {
  SaveError(errptr,
            db->rep->Merge(options->rep, column_family->rep,
              Slice(key, keylen), Slice(val, vallen)));
}

void cabindb_write(
    cabindb_t* db,
    const cabindb_writeoptions_t* options,
    cabindb_writebatch_t* batch,
    char** errptr) {
  SaveError(errptr, db->rep->Write(options->rep, &batch->rep));
}

char* cabindb_get(
    cabindb_t* db,
    const cabindb_readoptions_t* options,
    const char* key, size_t keylen,
    size_t* vallen,
    char** errptr) {
  char* result = nullptr;
  std::string tmp;
  Status s = db->rep->Get(options->rep, Slice(key, keylen), &tmp);
  if (s.ok()) {
    *vallen = tmp.size();
    result = CopyString(tmp);
  } else {
    *vallen = 0;
    if (!s.IsNotFound()) {
      SaveError(errptr, s);
    }
  }
  return result;
}

char* cabindb_get_cf(
    cabindb_t* db,
    const cabindb_readoptions_t* options,
    cabindb_column_family_handle_t* column_family,
    const char* key, size_t keylen,
    size_t* vallen,
    char** errptr) {
  char* result = nullptr;
  std::string tmp;
  Status s = db->rep->Get(options->rep, column_family->rep,
      Slice(key, keylen), &tmp);
  if (s.ok()) {
    *vallen = tmp.size();
    result = CopyString(tmp);
  } else {
    *vallen = 0;
    if (!s.IsNotFound()) {
      SaveError(errptr, s);
    }
  }
  return result;
}

void cabindb_multi_get(
    cabindb_t* db,
    const cabindb_readoptions_t* options,
    size_t num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes,
    char** values_list, size_t* values_list_sizes,
    char** errs) {
  std::vector<Slice> keys(num_keys);
  for (size_t i = 0; i < num_keys; i++) {
    keys[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  std::vector<std::string> values(num_keys);
  std::vector<Status> statuses = db->rep->MultiGet(options->rep, keys, &values);
  for (size_t i = 0; i < num_keys; i++) {
    if (statuses[i].ok()) {
      values_list[i] = CopyString(values[i]);
      values_list_sizes[i] = values[i].size();
      errs[i] = nullptr;
    } else {
      values_list[i] = nullptr;
      values_list_sizes[i] = 0;
      if (!statuses[i].IsNotFound()) {
        errs[i] = strdup(statuses[i].ToString().c_str());
      } else {
        errs[i] = nullptr;
      }
    }
  }
}

void cabindb_multi_get_cf(
    cabindb_t* db,
    const cabindb_readoptions_t* options,
    const cabindb_column_family_handle_t* const* column_families,
    size_t num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes,
    char** values_list, size_t* values_list_sizes,
    char** errs) {
  std::vector<Slice> keys(num_keys);
  std::vector<ColumnFamilyHandle*> cfs(num_keys);
  for (size_t i = 0; i < num_keys; i++) {
    keys[i] = Slice(keys_list[i], keys_list_sizes[i]);
    cfs[i] = column_families[i]->rep;
  }
  std::vector<std::string> values(num_keys);
  std::vector<Status> statuses = db->rep->MultiGet(options->rep, cfs, keys, &values);
  for (size_t i = 0; i < num_keys; i++) {
    if (statuses[i].ok()) {
      values_list[i] = CopyString(values[i]);
      values_list_sizes[i] = values[i].size();
      errs[i] = nullptr;
    } else {
      values_list[i] = nullptr;
      values_list_sizes[i] = 0;
      if (!statuses[i].IsNotFound()) {
        errs[i] = strdup(statuses[i].ToString().c_str());
      } else {
        errs[i] = nullptr;
      }
    }
  }
}

unsigned char cabindb_key_may_exist(cabindb_t* db,
                                    const cabindb_readoptions_t* options,
                                    const char* key, size_t key_len,
                                    char** value, size_t* val_len,
                                    const char* timestamp, size_t timestamp_len,
                                    unsigned char* value_found) {
  std::string tmp;
  std::string time;
  if (timestamp) {
    time.assign(timestamp, timestamp_len);
  }
  bool found = false;
  const bool result = db->rep->KeyMayExist(options->rep, Slice(key, key_len),
                                           &tmp, timestamp ? &time : nullptr,
                                           value_found ? &found : nullptr);
  if (value_found) {
    *value_found = found;
    if (found) {
      *val_len = tmp.size();
      *value = CopyString(tmp);
    }
  }
  return result;
}

unsigned char cabindb_key_may_exist_cf(
    cabindb_t* db, const cabindb_readoptions_t* options,
    cabindb_column_family_handle_t* column_family, const char* key,
    size_t key_len, char** value, size_t* val_len, const char* timestamp,
    size_t timestamp_len, unsigned char* value_found) {
  std::string tmp;
  std::string time;
  if (timestamp) {
    time.assign(timestamp, timestamp_len);
  }
  bool found = false;
  const bool result = db->rep->KeyMayExist(
      options->rep, column_family->rep, Slice(key, key_len), &tmp,
      timestamp ? &time : nullptr, value_found ? &found : nullptr);
  if (value_found) {
    *value_found = found;
    if (found) {
      *val_len = tmp.size();
      *value = CopyString(tmp);
    }
  }
  return result;
}

cabindb_iterator_t* cabindb_create_iterator(
    cabindb_t* db,
    const cabindb_readoptions_t* options) {
  cabindb_iterator_t* result = new cabindb_iterator_t;
  result->rep = db->rep->NewIterator(options->rep);
  return result;
}

cabindb_wal_iterator_t* cabindb_get_updates_since(
        cabindb_t* db, uint64_t seq_number,
        const cabindb_wal_readoptions_t* options,
        char** errptr) {
  std::unique_ptr<TransactionLogIterator> iter;
  TransactionLogIterator::ReadOptions ro;
  if (options!=nullptr) {
      ro = options->rep;
  }
  if (SaveError(errptr, db->rep->GetUpdatesSince(seq_number, &iter, ro))) {
    return nullptr;
  }
  cabindb_wal_iterator_t* result = new cabindb_wal_iterator_t;
  result->rep = iter.release();
  return result;
}

void cabindb_wal_iter_next(cabindb_wal_iterator_t* iter) {
    iter->rep->Next();
}

unsigned char cabindb_wal_iter_valid(const cabindb_wal_iterator_t* iter) {
    return iter->rep->Valid();
}

void cabindb_wal_iter_status (const cabindb_wal_iterator_t* iter, char** errptr) {
    SaveError(errptr, iter->rep->status());
}

void cabindb_wal_iter_destroy (const cabindb_wal_iterator_t* iter) {
  delete iter->rep;
  delete iter;
}

cabindb_writebatch_t* cabindb_wal_iter_get_batch (const cabindb_wal_iterator_t* iter, uint64_t* seq) {
  cabindb_writebatch_t* result = cabindb_writebatch_create();
  BatchResult wal_batch = iter->rep->GetBatch();
  result->rep = std::move(*wal_batch.writeBatchPtr);
  if (seq != nullptr) {
    *seq = wal_batch.sequence;
  }
  return result;
}

uint64_t cabindb_get_latest_sequence_number (cabindb_t *db) {
    return db->rep->GetLatestSequenceNumber();
}

cabindb_iterator_t* cabindb_create_iterator_cf(
    cabindb_t* db,
    const cabindb_readoptions_t* options,
    cabindb_column_family_handle_t* column_family) {
  cabindb_iterator_t* result = new cabindb_iterator_t;
  result->rep = db->rep->NewIterator(options->rep, column_family->rep);
  return result;
}

void cabindb_create_iterators(
    cabindb_t *db,
    cabindb_readoptions_t* opts,
    cabindb_column_family_handle_t** column_families,
    cabindb_iterator_t** iterators,
    size_t size,
    char** errptr) {
  std::vector<ColumnFamilyHandle*> column_families_vec;
  for (size_t i = 0; i < size; i++) {
    column_families_vec.push_back(column_families[i]->rep);
  }

  std::vector<Iterator*> res;
  Status status = db->rep->NewIterators(opts->rep, column_families_vec, &res);
  assert(res.size() == size);
  if (SaveError(errptr, status)) {
    return;
  }

  for (size_t i = 0; i < size; i++) {
    iterators[i] = new cabindb_iterator_t;
    iterators[i]->rep = res[i];
  }
}

const cabindb_snapshot_t* cabindb_create_snapshot(
    cabindb_t* db) {
  cabindb_snapshot_t* result = new cabindb_snapshot_t;
  result->rep = db->rep->GetSnapshot();
  return result;
}

void cabindb_release_snapshot(
    cabindb_t* db,
    const cabindb_snapshot_t* snapshot) {
  db->rep->ReleaseSnapshot(snapshot->rep);
  delete snapshot;
}

char* cabindb_property_value(
    cabindb_t* db,
    const char* propname) {
  std::string tmp;
  if (db->rep->GetProperty(Slice(propname), &tmp)) {
    // We use strdup() since we expect human readable output.
    return strdup(tmp.c_str());
  } else {
    return nullptr;
  }
}

int cabindb_property_int(
    cabindb_t* db,
    const char* propname,
    uint64_t *out_val) {
  if (db->rep->GetIntProperty(Slice(propname), out_val)) {
    return 0;
  } else {
    return -1;
  }
}

int cabindb_property_int_cf(
    cabindb_t* db,
    cabindb_column_family_handle_t* column_family,
    const char* propname,
    uint64_t *out_val) {
  if (db->rep->GetIntProperty(column_family->rep, Slice(propname), out_val)) {
    return 0;
  } else {
    return -1;
  }
}

char* cabindb_property_value_cf(
    cabindb_t* db,
    cabindb_column_family_handle_t* column_family,
    const char* propname) {
  std::string tmp;
  if (db->rep->GetProperty(column_family->rep, Slice(propname), &tmp)) {
    // We use strdup() since we expect human readable output.
    return strdup(tmp.c_str());
  } else {
    return nullptr;
  }
}

void cabindb_approximate_sizes(
    cabindb_t* db,
    int num_ranges,
    const char* const* range_start_key, const size_t* range_start_key_len,
    const char* const* range_limit_key, const size_t* range_limit_key_len,
    uint64_t* sizes) {
  Range* ranges = new Range[num_ranges];
  for (int i = 0; i < num_ranges; i++) {
    ranges[i].start = Slice(range_start_key[i], range_start_key_len[i]);
    ranges[i].limit = Slice(range_limit_key[i], range_limit_key_len[i]);
  }
  db->rep->GetApproximateSizes(ranges, num_ranges, sizes);
  delete[] ranges;
}

void cabindb_approximate_sizes_cf(
    cabindb_t* db,
    cabindb_column_family_handle_t* column_family,
    int num_ranges,
    const char* const* range_start_key, const size_t* range_start_key_len,
    const char* const* range_limit_key, const size_t* range_limit_key_len,
    uint64_t* sizes) {
  Range* ranges = new Range[num_ranges];
  for (int i = 0; i < num_ranges; i++) {
    ranges[i].start = Slice(range_start_key[i], range_start_key_len[i]);
    ranges[i].limit = Slice(range_limit_key[i], range_limit_key_len[i]);
  }
  db->rep->GetApproximateSizes(column_family->rep, ranges, num_ranges, sizes);
  delete[] ranges;
}

void cabindb_delete_file(
    cabindb_t* db,
    const char* name) {
  db->rep->DeleteFile(name);
}

const cabindb_livefiles_t* cabindb_livefiles(
    cabindb_t* db) {
  cabindb_livefiles_t* result = new cabindb_livefiles_t;
  db->rep->GetLiveFilesMetaData(&result->rep);
  return result;
}

void cabindb_compact_range(
    cabindb_t* db,
    const char* start_key, size_t start_key_len,
    const char* limit_key, size_t limit_key_len) {
  Slice a, b;
  db->rep->CompactRange(
      CompactRangeOptions(),
      // Pass nullptr Slice if corresponding "const char*" is nullptr
      (start_key ? (a = Slice(start_key, start_key_len), &a) : nullptr),
      (limit_key ? (b = Slice(limit_key, limit_key_len), &b) : nullptr));
}

void cabindb_compact_range_cf(
    cabindb_t* db,
    cabindb_column_family_handle_t* column_family,
    const char* start_key, size_t start_key_len,
    const char* limit_key, size_t limit_key_len) {
  Slice a, b;
  db->rep->CompactRange(
      CompactRangeOptions(), column_family->rep,
      // Pass nullptr Slice if corresponding "const char*" is nullptr
      (start_key ? (a = Slice(start_key, start_key_len), &a) : nullptr),
      (limit_key ? (b = Slice(limit_key, limit_key_len), &b) : nullptr));
}

void cabindb_compact_range_opt(cabindb_t* db, cabindb_compactoptions_t* opt,
                               const char* start_key, size_t start_key_len,
                               const char* limit_key, size_t limit_key_len) {
  Slice a, b;
  db->rep->CompactRange(
      opt->rep,
      // Pass nullptr Slice if corresponding "const char*" is nullptr
      (start_key ? (a = Slice(start_key, start_key_len), &a) : nullptr),
      (limit_key ? (b = Slice(limit_key, limit_key_len), &b) : nullptr));
}

void cabindb_compact_range_cf_opt(cabindb_t* db,
                                  cabindb_column_family_handle_t* column_family,
                                  cabindb_compactoptions_t* opt,
                                  const char* start_key, size_t start_key_len,
                                  const char* limit_key, size_t limit_key_len) {
  Slice a, b;
  db->rep->CompactRange(
      opt->rep, column_family->rep,
      // Pass nullptr Slice if corresponding "const char*" is nullptr
      (start_key ? (a = Slice(start_key, start_key_len), &a) : nullptr),
      (limit_key ? (b = Slice(limit_key, limit_key_len), &b) : nullptr));
}

void cabindb_flush(
    cabindb_t* db,
    const cabindb_flushoptions_t* options,
    char** errptr) {
  SaveError(errptr, db->rep->Flush(options->rep));
}

void cabindb_flush_cf(
    cabindb_t* db,
    const cabindb_flushoptions_t* options,
    cabindb_column_family_handle_t* column_family,
    char** errptr) {
  SaveError(errptr, db->rep->Flush(options->rep, column_family->rep));
}

void cabindb_disable_file_deletions(
    cabindb_t* db,
    char** errptr) {
  SaveError(errptr, db->rep->DisableFileDeletions());
}

void cabindb_enable_file_deletions(
    cabindb_t* db,
    unsigned char force,
    char** errptr) {
  SaveError(errptr, db->rep->EnableFileDeletions(force));
}

void cabindb_destroy_db(
    const cabindb_options_t* options,
    const char* name,
    char** errptr) {
  SaveError(errptr, DestroyDB(name, options->rep));
}

void cabindb_repair_db(
    const cabindb_options_t* options,
    const char* name,
    char** errptr) {
  SaveError(errptr, RepairDB(name, options->rep));
}

void cabindb_iter_destroy(cabindb_iterator_t* iter) {
  delete iter->rep;
  delete iter;
}

unsigned char cabindb_iter_valid(const cabindb_iterator_t* iter) {
  return iter->rep->Valid();
}

void cabindb_iter_seek_to_first(cabindb_iterator_t* iter) {
  iter->rep->SeekToFirst();
}

void cabindb_iter_seek_to_last(cabindb_iterator_t* iter) {
  iter->rep->SeekToLast();
}

void cabindb_iter_seek(cabindb_iterator_t* iter, const char* k, size_t klen) {
  iter->rep->Seek(Slice(k, klen));
}

void cabindb_iter_seek_for_prev(cabindb_iterator_t* iter, const char* k,
                                size_t klen) {
  iter->rep->SeekForPrev(Slice(k, klen));
}

void cabindb_iter_next(cabindb_iterator_t* iter) {
  iter->rep->Next();
}

void cabindb_iter_prev(cabindb_iterator_t* iter) {
  iter->rep->Prev();
}

const char* cabindb_iter_key(const cabindb_iterator_t* iter, size_t* klen) {
  Slice s = iter->rep->key();
  *klen = s.size();
  return s.data();
}

const char* cabindb_iter_value(const cabindb_iterator_t* iter, size_t* vlen) {
  Slice s = iter->rep->value();
  *vlen = s.size();
  return s.data();
}

void cabindb_iter_get_error(const cabindb_iterator_t* iter, char** errptr) {
  SaveError(errptr, iter->rep->status());
}

cabindb_writebatch_t* cabindb_writebatch_create() {
  return new cabindb_writebatch_t;
}

cabindb_writebatch_t* cabindb_writebatch_create_from(const char* rep,
                                                     size_t size) {
  cabindb_writebatch_t* b = new cabindb_writebatch_t;
  b->rep = WriteBatch(std::string(rep, size));
  return b;
}

void cabindb_writebatch_destroy(cabindb_writebatch_t* b) {
  delete b;
}

void cabindb_writebatch_clear(cabindb_writebatch_t* b) {
  b->rep.Clear();
}

int cabindb_writebatch_count(cabindb_writebatch_t* b) {
  return b->rep.Count();
}

void cabindb_writebatch_put(
    cabindb_writebatch_t* b,
    const char* key, size_t klen,
    const char* val, size_t vlen) {
  b->rep.Put(Slice(key, klen), Slice(val, vlen));
}

void cabindb_writebatch_put_cf(
    cabindb_writebatch_t* b,
    cabindb_column_family_handle_t* column_family,
    const char* key, size_t klen,
    const char* val, size_t vlen) {
  b->rep.Put(column_family->rep, Slice(key, klen), Slice(val, vlen));
}

void cabindb_writebatch_putv(
    cabindb_writebatch_t* b,
    int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes,
    int num_values, const char* const* values_list,
    const size_t* values_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  std::vector<Slice> value_slices(num_values);
  for (int i = 0; i < num_values; i++) {
    value_slices[i] = Slice(values_list[i], values_list_sizes[i]);
  }
  b->rep.Put(SliceParts(key_slices.data(), num_keys),
             SliceParts(value_slices.data(), num_values));
}

void cabindb_writebatch_putv_cf(
    cabindb_writebatch_t* b,
    cabindb_column_family_handle_t* column_family,
    int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes,
    int num_values, const char* const* values_list,
    const size_t* values_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  std::vector<Slice> value_slices(num_values);
  for (int i = 0; i < num_values; i++) {
    value_slices[i] = Slice(values_list[i], values_list_sizes[i]);
  }
  b->rep.Put(column_family->rep, SliceParts(key_slices.data(), num_keys),
             SliceParts(value_slices.data(), num_values));
}

void cabindb_writebatch_merge(
    cabindb_writebatch_t* b,
    const char* key, size_t klen,
    const char* val, size_t vlen) {
  b->rep.Merge(Slice(key, klen), Slice(val, vlen));
}

void cabindb_writebatch_merge_cf(
    cabindb_writebatch_t* b,
    cabindb_column_family_handle_t* column_family,
    const char* key, size_t klen,
    const char* val, size_t vlen) {
  b->rep.Merge(column_family->rep, Slice(key, klen), Slice(val, vlen));
}

void cabindb_writebatch_mergev(
    cabindb_writebatch_t* b,
    int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes,
    int num_values, const char* const* values_list,
    const size_t* values_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  std::vector<Slice> value_slices(num_values);
  for (int i = 0; i < num_values; i++) {
    value_slices[i] = Slice(values_list[i], values_list_sizes[i]);
  }
  b->rep.Merge(SliceParts(key_slices.data(), num_keys),
               SliceParts(value_slices.data(), num_values));
}

void cabindb_writebatch_mergev_cf(
    cabindb_writebatch_t* b,
    cabindb_column_family_handle_t* column_family,
    int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes,
    int num_values, const char* const* values_list,
    const size_t* values_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  std::vector<Slice> value_slices(num_values);
  for (int i = 0; i < num_values; i++) {
    value_slices[i] = Slice(values_list[i], values_list_sizes[i]);
  }
  b->rep.Merge(column_family->rep, SliceParts(key_slices.data(), num_keys),
               SliceParts(value_slices.data(), num_values));
}

void cabindb_writebatch_delete(
    cabindb_writebatch_t* b,
    const char* key, size_t klen) {
  b->rep.Delete(Slice(key, klen));
}

void cabindb_writebatch_singledelete(cabindb_writebatch_t* b, const char* key,
                                     size_t klen) {
  b->rep.SingleDelete(Slice(key, klen));
}

void cabindb_writebatch_delete_cf(
    cabindb_writebatch_t* b,
    cabindb_column_family_handle_t* column_family,
    const char* key, size_t klen) {
  b->rep.Delete(column_family->rep, Slice(key, klen));
}

void cabindb_writebatch_singledelete_cf(
    cabindb_writebatch_t* b, cabindb_column_family_handle_t* column_family,
    const char* key, size_t klen) {
  b->rep.SingleDelete(column_family->rep, Slice(key, klen));
}

void cabindb_writebatch_deletev(
    cabindb_writebatch_t* b,
    int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  b->rep.Delete(SliceParts(key_slices.data(), num_keys));
}

void cabindb_writebatch_deletev_cf(
    cabindb_writebatch_t* b,
    cabindb_column_family_handle_t* column_family,
    int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  b->rep.Delete(column_family->rep, SliceParts(key_slices.data(), num_keys));
}

void cabindb_writebatch_delete_range(cabindb_writebatch_t* b,
                                     const char* start_key,
                                     size_t start_key_len, const char* end_key,
                                     size_t end_key_len) {
  b->rep.DeleteRange(Slice(start_key, start_key_len),
                     Slice(end_key, end_key_len));
}

void cabindb_writebatch_delete_range_cf(
    cabindb_writebatch_t* b, cabindb_column_family_handle_t* column_family,
    const char* start_key, size_t start_key_len, const char* end_key,
    size_t end_key_len) {
  b->rep.DeleteRange(column_family->rep, Slice(start_key, start_key_len),
                     Slice(end_key, end_key_len));
}

void cabindb_writebatch_delete_rangev(cabindb_writebatch_t* b, int num_keys,
                                      const char* const* start_keys_list,
                                      const size_t* start_keys_list_sizes,
                                      const char* const* end_keys_list,
                                      const size_t* end_keys_list_sizes) {
  std::vector<Slice> start_key_slices(num_keys);
  std::vector<Slice> end_key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    start_key_slices[i] = Slice(start_keys_list[i], start_keys_list_sizes[i]);
    end_key_slices[i] = Slice(end_keys_list[i], end_keys_list_sizes[i]);
  }
  b->rep.DeleteRange(SliceParts(start_key_slices.data(), num_keys),
                     SliceParts(end_key_slices.data(), num_keys));
}

void cabindb_writebatch_delete_rangev_cf(
    cabindb_writebatch_t* b, cabindb_column_family_handle_t* column_family,
    int num_keys, const char* const* start_keys_list,
    const size_t* start_keys_list_sizes, const char* const* end_keys_list,
    const size_t* end_keys_list_sizes) {
  std::vector<Slice> start_key_slices(num_keys);
  std::vector<Slice> end_key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    start_key_slices[i] = Slice(start_keys_list[i], start_keys_list_sizes[i]);
    end_key_slices[i] = Slice(end_keys_list[i], end_keys_list_sizes[i]);
  }
  b->rep.DeleteRange(column_family->rep,
                     SliceParts(start_key_slices.data(), num_keys),
                     SliceParts(end_key_slices.data(), num_keys));
}

void cabindb_writebatch_put_log_data(
    cabindb_writebatch_t* b,
    const char* blob, size_t len) {
  b->rep.PutLogData(Slice(blob, len));
}

class H : public WriteBatch::Handler {
 public:
  void* state_;
  void (*put_)(void*, const char* k, size_t klen, const char* v, size_t vlen);
  void (*deleted_)(void*, const char* k, size_t klen);
  void Put(const Slice& key, const Slice& value) override {
    (*put_)(state_, key.data(), key.size(), value.data(), value.size());
  }
  void Delete(const Slice& key) override {
    (*deleted_)(state_, key.data(), key.size());
  }
};

void cabindb_writebatch_iterate(
    cabindb_writebatch_t* b,
    void* state,
    void (*put)(void*, const char* k, size_t klen, const char* v, size_t vlen),
    void (*deleted)(void*, const char* k, size_t klen)) {
  H handler;
  handler.state_ = state;
  handler.put_ = put;
  handler.deleted_ = deleted;
  b->rep.Iterate(&handler);
}

const char* cabindb_writebatch_data(cabindb_writebatch_t* b, size_t* size) {
  *size = b->rep.GetDataSize();
  return b->rep.Data().c_str();
}

void cabindb_writebatch_set_save_point(cabindb_writebatch_t* b) {
  b->rep.SetSavePoint();
}

void cabindb_writebatch_rollback_to_save_point(cabindb_writebatch_t* b,
                                               char** errptr) {
  SaveError(errptr, b->rep.RollbackToSavePoint());
}

void cabindb_writebatch_pop_save_point(cabindb_writebatch_t* b, char** errptr) {
  SaveError(errptr, b->rep.PopSavePoint());
}

cabindb_writebatch_wi_t* cabindb_writebatch_wi_create(size_t reserved_bytes, unsigned char overwrite_key) {
  cabindb_writebatch_wi_t* b = new cabindb_writebatch_wi_t;
  b->rep = new WriteBatchWithIndex(BytewiseComparator(), reserved_bytes, overwrite_key);
  return b;
}

void cabindb_writebatch_wi_destroy(cabindb_writebatch_wi_t* b) {
  if (b->rep) {
    delete b->rep;
  }
  delete b;
}

void cabindb_writebatch_wi_clear(cabindb_writebatch_wi_t* b) {
  b->rep->Clear();
}

int cabindb_writebatch_wi_count(cabindb_writebatch_wi_t* b) {
  return b->rep->GetWriteBatch()->Count();
}

void cabindb_writebatch_wi_put(
    cabindb_writebatch_wi_t* b,
    const char* key, size_t klen,
    const char* val, size_t vlen) {
  b->rep->Put(Slice(key, klen), Slice(val, vlen));
}

void cabindb_writebatch_wi_put_cf(
    cabindb_writebatch_wi_t* b,
    cabindb_column_family_handle_t* column_family,
    const char* key, size_t klen,
    const char* val, size_t vlen) {
  b->rep->Put(column_family->rep, Slice(key, klen), Slice(val, vlen));
}

void cabindb_writebatch_wi_putv(
    cabindb_writebatch_wi_t* b,
    int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes,
    int num_values, const char* const* values_list,
    const size_t* values_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  std::vector<Slice> value_slices(num_values);
  for (int i = 0; i < num_values; i++) {
    value_slices[i] = Slice(values_list[i], values_list_sizes[i]);
  }
  b->rep->Put(SliceParts(key_slices.data(), num_keys),
             SliceParts(value_slices.data(), num_values));
}

void cabindb_writebatch_wi_putv_cf(
    cabindb_writebatch_wi_t* b,
    cabindb_column_family_handle_t* column_family,
    int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes,
    int num_values, const char* const* values_list,
    const size_t* values_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  std::vector<Slice> value_slices(num_values);
  for (int i = 0; i < num_values; i++) {
    value_slices[i] = Slice(values_list[i], values_list_sizes[i]);
  }
  b->rep->Put(column_family->rep, SliceParts(key_slices.data(), num_keys),
             SliceParts(value_slices.data(), num_values));
}

void cabindb_writebatch_wi_merge(
    cabindb_writebatch_wi_t* b,
    const char* key, size_t klen,
    const char* val, size_t vlen) {
  b->rep->Merge(Slice(key, klen), Slice(val, vlen));
}

void cabindb_writebatch_wi_merge_cf(
    cabindb_writebatch_wi_t* b,
    cabindb_column_family_handle_t* column_family,
    const char* key, size_t klen,
    const char* val, size_t vlen) {
  b->rep->Merge(column_family->rep, Slice(key, klen), Slice(val, vlen));
}

void cabindb_writebatch_wi_mergev(
    cabindb_writebatch_wi_t* b,
    int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes,
    int num_values, const char* const* values_list,
    const size_t* values_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  std::vector<Slice> value_slices(num_values);
  for (int i = 0; i < num_values; i++) {
    value_slices[i] = Slice(values_list[i], values_list_sizes[i]);
  }
  b->rep->Merge(SliceParts(key_slices.data(), num_keys),
               SliceParts(value_slices.data(), num_values));
}

void cabindb_writebatch_wi_mergev_cf(
    cabindb_writebatch_wi_t* b,
    cabindb_column_family_handle_t* column_family,
    int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes,
    int num_values, const char* const* values_list,
    const size_t* values_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  std::vector<Slice> value_slices(num_values);
  for (int i = 0; i < num_values; i++) {
    value_slices[i] = Slice(values_list[i], values_list_sizes[i]);
  }
  b->rep->Merge(column_family->rep, SliceParts(key_slices.data(), num_keys),
               SliceParts(value_slices.data(), num_values));
}

void cabindb_writebatch_wi_delete(
    cabindb_writebatch_wi_t* b,
    const char* key, size_t klen) {
  b->rep->Delete(Slice(key, klen));
}

void cabindb_writebatch_wi_singledelete(cabindb_writebatch_wi_t* b,
                                        const char* key, size_t klen) {
  b->rep->SingleDelete(Slice(key, klen));
}

void cabindb_writebatch_wi_delete_cf(
    cabindb_writebatch_wi_t* b,
    cabindb_column_family_handle_t* column_family,
    const char* key, size_t klen) {
  b->rep->Delete(column_family->rep, Slice(key, klen));
}

void cabindb_writebatch_wi_singledelete_cf(
    cabindb_writebatch_wi_t* b, cabindb_column_family_handle_t* column_family,
    const char* key, size_t klen) {
  b->rep->SingleDelete(column_family->rep, Slice(key, klen));
}

void cabindb_writebatch_wi_deletev(
    cabindb_writebatch_wi_t* b,
    int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  b->rep->Delete(SliceParts(key_slices.data(), num_keys));
}

void cabindb_writebatch_wi_deletev_cf(
    cabindb_writebatch_wi_t* b,
    cabindb_column_family_handle_t* column_family,
    int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  b->rep->Delete(column_family->rep, SliceParts(key_slices.data(), num_keys));
}

void cabindb_writebatch_wi_delete_range(cabindb_writebatch_wi_t* b,
                                     const char* start_key,
                                     size_t start_key_len, const char* end_key,
                                     size_t end_key_len) {
  b->rep->DeleteRange(Slice(start_key, start_key_len),
                     Slice(end_key, end_key_len));
}

void cabindb_writebatch_wi_delete_range_cf(
    cabindb_writebatch_wi_t* b, cabindb_column_family_handle_t* column_family,
    const char* start_key, size_t start_key_len, const char* end_key,
    size_t end_key_len) {
  b->rep->DeleteRange(column_family->rep, Slice(start_key, start_key_len),
                     Slice(end_key, end_key_len));
}

void cabindb_writebatch_wi_delete_rangev(cabindb_writebatch_wi_t* b, int num_keys,
                                      const char* const* start_keys_list,
                                      const size_t* start_keys_list_sizes,
                                      const char* const* end_keys_list,
                                      const size_t* end_keys_list_sizes) {
  std::vector<Slice> start_key_slices(num_keys);
  std::vector<Slice> end_key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    start_key_slices[i] = Slice(start_keys_list[i], start_keys_list_sizes[i]);
    end_key_slices[i] = Slice(end_keys_list[i], end_keys_list_sizes[i]);
  }
  b->rep->DeleteRange(SliceParts(start_key_slices.data(), num_keys),
                     SliceParts(end_key_slices.data(), num_keys));
}

void cabindb_writebatch_wi_delete_rangev_cf(
    cabindb_writebatch_wi_t* b, cabindb_column_family_handle_t* column_family,
    int num_keys, const char* const* start_keys_list,
    const size_t* start_keys_list_sizes, const char* const* end_keys_list,
    const size_t* end_keys_list_sizes) {
  std::vector<Slice> start_key_slices(num_keys);
  std::vector<Slice> end_key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    start_key_slices[i] = Slice(start_keys_list[i], start_keys_list_sizes[i]);
    end_key_slices[i] = Slice(end_keys_list[i], end_keys_list_sizes[i]);
  }
  b->rep->DeleteRange(column_family->rep,
                     SliceParts(start_key_slices.data(), num_keys),
                     SliceParts(end_key_slices.data(), num_keys));
}

void cabindb_writebatch_wi_put_log_data(
    cabindb_writebatch_wi_t* b,
    const char* blob, size_t len) {
  b->rep->PutLogData(Slice(blob, len));
}

void cabindb_writebatch_wi_iterate(
    cabindb_writebatch_wi_t* b,
    void* state,
    void (*put)(void*, const char* k, size_t klen, const char* v, size_t vlen),
    void (*deleted)(void*, const char* k, size_t klen)) {
  H handler;
  handler.state_ = state;
  handler.put_ = put;
  handler.deleted_ = deleted;
  b->rep->GetWriteBatch()->Iterate(&handler);
}

const char* cabindb_writebatch_wi_data(cabindb_writebatch_wi_t* b, size_t* size) {
  WriteBatch* wb = b->rep->GetWriteBatch();
  *size = wb->GetDataSize();
  return wb->Data().c_str();
}

void cabindb_writebatch_wi_set_save_point(cabindb_writebatch_wi_t* b) {
  b->rep->SetSavePoint();
}

void cabindb_writebatch_wi_rollback_to_save_point(cabindb_writebatch_wi_t* b,
                                               char** errptr) {
  SaveError(errptr, b->rep->RollbackToSavePoint());
}

cabindb_iterator_t* cabindb_writebatch_wi_create_iterator_with_base(
    cabindb_writebatch_wi_t* wbwi,
    cabindb_iterator_t* base_iterator) {
  cabindb_iterator_t* result = new cabindb_iterator_t;
  result->rep = wbwi->rep->NewIteratorWithBase(base_iterator->rep);
  delete base_iterator;
  return result;
}

cabindb_iterator_t* cabindb_writebatch_wi_create_iterator_with_base_cf(
    cabindb_writebatch_wi_t* wbwi, cabindb_iterator_t* base_iterator,
    cabindb_column_family_handle_t* column_family) {
  cabindb_iterator_t* result = new cabindb_iterator_t;
  result->rep =
      wbwi->rep->NewIteratorWithBase(column_family->rep, base_iterator->rep);
  delete base_iterator;
  return result;
}

char* cabindb_writebatch_wi_get_from_batch(
    cabindb_writebatch_wi_t* wbwi,
    const cabindb_options_t* options,
    const char* key, size_t keylen,
    size_t* vallen,
    char** errptr) {
  char* result = nullptr;
  std::string tmp;
  Status s = wbwi->rep->GetFromBatch(options->rep, Slice(key, keylen), &tmp);
  if (s.ok()) {
    *vallen = tmp.size();
    result = CopyString(tmp);
  } else {
    *vallen = 0;
    if (!s.IsNotFound()) {
      SaveError(errptr, s);
    }
  }
  return result;
}

char* cabindb_writebatch_wi_get_from_batch_cf(
    cabindb_writebatch_wi_t* wbwi,
    const cabindb_options_t* options,
    cabindb_column_family_handle_t* column_family,
    const char* key, size_t keylen,
    size_t* vallen,
    char** errptr) {
  char* result = nullptr;
  std::string tmp;
  Status s = wbwi->rep->GetFromBatch(column_family->rep, options->rep,
      Slice(key, keylen), &tmp);
  if (s.ok()) {
    *vallen = tmp.size();
    result = CopyString(tmp);
  } else {
    *vallen = 0;
    if (!s.IsNotFound()) {
      SaveError(errptr, s);
    }
  }
  return result;
}

char* cabindb_writebatch_wi_get_from_batch_and_db(
    cabindb_writebatch_wi_t* wbwi,
    cabindb_t* db,
    const cabindb_readoptions_t* options,
    const char* key, size_t keylen,
    size_t* vallen,
    char** errptr) {
  char* result = nullptr;
  std::string tmp;
  Status s = wbwi->rep->GetFromBatchAndDB(db->rep, options->rep, Slice(key, keylen), &tmp);
  if (s.ok()) {
    *vallen = tmp.size();
    result = CopyString(tmp);
  } else {
    *vallen = 0;
    if (!s.IsNotFound()) {
      SaveError(errptr, s);
    }
  }
  return result;
}

char* cabindb_writebatch_wi_get_from_batch_and_db_cf(
    cabindb_writebatch_wi_t* wbwi,
    cabindb_t* db,
    const cabindb_readoptions_t* options,
    cabindb_column_family_handle_t* column_family,
    const char* key, size_t keylen,
    size_t* vallen,
    char** errptr) {
  char* result = nullptr;
  std::string tmp;
  Status s = wbwi->rep->GetFromBatchAndDB(db->rep, options->rep, column_family->rep,
      Slice(key, keylen), &tmp);
  if (s.ok()) {
    *vallen = tmp.size();
    result = CopyString(tmp);
  } else {
    *vallen = 0;
    if (!s.IsNotFound()) {
      SaveError(errptr, s);
    }
  }
  return result;
}

void cabindb_write_writebatch_wi(
    cabindb_t* db,
    const cabindb_writeoptions_t* options,
    cabindb_writebatch_wi_t* wbwi,
    char** errptr) {
  WriteBatch* wb = wbwi->rep->GetWriteBatch();
  SaveError(errptr, db->rep->Write(options->rep, wb));
}

cabindb_block_based_table_options_t*
cabindb_block_based_options_create() {
  return new cabindb_block_based_table_options_t;
}

void cabindb_block_based_options_destroy(
    cabindb_block_based_table_options_t* options) {
  delete options;
}

void cabindb_block_based_options_set_block_size(
    cabindb_block_based_table_options_t* options, size_t block_size) {
  options->rep.block_size = block_size;
}

void cabindb_block_based_options_set_block_size_deviation(
    cabindb_block_based_table_options_t* options, int block_size_deviation) {
  options->rep.block_size_deviation = block_size_deviation;
}

void cabindb_block_based_options_set_block_restart_interval(
    cabindb_block_based_table_options_t* options, int block_restart_interval) {
  options->rep.block_restart_interval = block_restart_interval;
}

void cabindb_block_based_options_set_index_block_restart_interval(
    cabindb_block_based_table_options_t* options, int index_block_restart_interval) {
  options->rep.index_block_restart_interval = index_block_restart_interval;
}

void cabindb_block_based_options_set_metadata_block_size(
    cabindb_block_based_table_options_t* options, uint64_t metadata_block_size) {
  options->rep.metadata_block_size = metadata_block_size;
}

void cabindb_block_based_options_set_partition_filters(
    cabindb_block_based_table_options_t* options, unsigned char partition_filters) {
  options->rep.partition_filters = partition_filters;
}

void cabindb_block_based_options_set_use_delta_encoding(
    cabindb_block_based_table_options_t* options, unsigned char use_delta_encoding) {
  options->rep.use_delta_encoding = use_delta_encoding;
}

void cabindb_block_based_options_set_filter_policy(
    cabindb_block_based_table_options_t* options,
    cabindb_filterpolicy_t* filter_policy) {
  options->rep.filter_policy.reset(filter_policy);
}

void cabindb_block_based_options_set_no_block_cache(
    cabindb_block_based_table_options_t* options,
    unsigned char no_block_cache) {
  options->rep.no_block_cache = no_block_cache;
}

void cabindb_block_based_options_set_block_cache(
    cabindb_block_based_table_options_t* options,
    cabindb_cache_t* block_cache) {
  if (block_cache) {
    options->rep.block_cache = block_cache->rep;
  }
}

void cabindb_block_based_options_set_block_cache_compressed(
    cabindb_block_based_table_options_t* options,
    cabindb_cache_t* block_cache_compressed) {
  if (block_cache_compressed) {
    options->rep.block_cache_compressed = block_cache_compressed->rep;
  }
}

void cabindb_block_based_options_set_whole_key_filtering(
    cabindb_block_based_table_options_t* options, unsigned char v) {
  options->rep.whole_key_filtering = v;
}

void cabindb_block_based_options_set_format_version(
    cabindb_block_based_table_options_t* options, int v) {
  options->rep.format_version = v;
}

void cabindb_block_based_options_set_index_type(
    cabindb_block_based_table_options_t* options, int v) {
  options->rep.index_type = static_cast<BlockBasedTableOptions::IndexType>(v);
}

void cabindb_block_based_options_set_data_block_index_type(
    cabindb_block_based_table_options_t* options, int v) {
  options->rep.data_block_index_type =
          static_cast<BlockBasedTableOptions::DataBlockIndexType>(v);
}

void cabindb_block_based_options_set_data_block_hash_ratio(
    cabindb_block_based_table_options_t* options, double v) {
  options->rep.data_block_hash_table_util_ratio = v;
}

void cabindb_block_based_options_set_hash_index_allow_collision(
    cabindb_block_based_table_options_t* options, unsigned char v) {
  options->rep.hash_index_allow_collision = v;
}

void cabindb_block_based_options_set_cache_index_and_filter_blocks(
    cabindb_block_based_table_options_t* options, unsigned char v) {
  options->rep.cache_index_and_filter_blocks = v;
}

void cabindb_block_based_options_set_cache_index_and_filter_blocks_with_high_priority(
    cabindb_block_based_table_options_t* options, unsigned char v) {
  options->rep.cache_index_and_filter_blocks_with_high_priority = v;
}

void cabindb_block_based_options_set_pin_l0_filter_and_index_blocks_in_cache(
    cabindb_block_based_table_options_t* options, unsigned char v) {
  options->rep.pin_l0_filter_and_index_blocks_in_cache = v;
}

void cabindb_block_based_options_set_pin_top_level_index_and_filter(
    cabindb_block_based_table_options_t* options, unsigned char v) {
  options->rep.pin_top_level_index_and_filter = v;
}

void cabindb_options_set_block_based_table_factory(
    cabindb_options_t *opt,
    cabindb_block_based_table_options_t* table_options) {
  if (table_options) {
    opt->rep.table_factory.reset(
        CABINDB_NAMESPACE::NewBlockBasedTableFactory(table_options->rep));
  }
}

cabindb_cuckoo_table_options_t*
cabindb_cuckoo_options_create() {
  return new cabindb_cuckoo_table_options_t;
}

void cabindb_cuckoo_options_destroy(
    cabindb_cuckoo_table_options_t* options) {
  delete options;
}

void cabindb_cuckoo_options_set_hash_ratio(
    cabindb_cuckoo_table_options_t* options, double v) {
  options->rep.hash_table_ratio = v;
}

void cabindb_cuckoo_options_set_max_search_depth(
    cabindb_cuckoo_table_options_t* options, uint32_t v) {
  options->rep.max_search_depth = v;
}

void cabindb_cuckoo_options_set_cuckoo_block_size(
    cabindb_cuckoo_table_options_t* options, uint32_t v) {
  options->rep.cuckoo_block_size = v;
}

void cabindb_cuckoo_options_set_identity_as_first_hash(
    cabindb_cuckoo_table_options_t* options, unsigned char v) {
  options->rep.identity_as_first_hash = v;
}

void cabindb_cuckoo_options_set_use_module_hash(
    cabindb_cuckoo_table_options_t* options, unsigned char v) {
  options->rep.use_module_hash = v;
}

void cabindb_options_set_cuckoo_table_factory(
    cabindb_options_t *opt,
    cabindb_cuckoo_table_options_t* table_options) {
  if (table_options) {
    opt->rep.table_factory.reset(
        CABINDB_NAMESPACE::NewCuckooTableFactory(table_options->rep));
  }
}

void cabindb_set_options(
    cabindb_t* db, int count, const char* const keys[], const char* const values[], char** errptr) {
        std::unordered_map<std::string, std::string> options_map;
        for (int i=0; i<count; i++)
            options_map[keys[i]] = values[i];
        SaveError(errptr,
            db->rep->SetOptions(options_map));
    }

void cabindb_set_options_cf(
    cabindb_t* db, cabindb_column_family_handle_t* handle, int count, const char* const keys[], const char* const values[], char** errptr) {
        std::unordered_map<std::string, std::string> options_map;
        for (int i=0; i<count; i++)
            options_map[keys[i]] = values[i];
        SaveError(errptr,
            db->rep->SetOptions(handle->rep, options_map));
    }

cabindb_options_t* cabindb_options_create() {
  return new cabindb_options_t;
}

void cabindb_options_destroy(cabindb_options_t* options) {
  delete options;
}

cabindb_options_t* cabindb_options_create_copy(cabindb_options_t* options) {
  return new cabindb_options_t(*options);
}

void cabindb_options_increase_parallelism(
    cabindb_options_t* opt, int total_threads) {
  opt->rep.IncreaseParallelism(total_threads);
}

void cabindb_options_optimize_for_point_lookup(
    cabindb_options_t* opt, uint64_t block_cache_size_mb) {
  opt->rep.OptimizeForPointLookup(block_cache_size_mb);
}

void cabindb_options_optimize_level_style_compaction(
    cabindb_options_t* opt, uint64_t memtable_memory_budget) {
  opt->rep.OptimizeLevelStyleCompaction(memtable_memory_budget);
}

void cabindb_options_optimize_universal_style_compaction(
    cabindb_options_t* opt, uint64_t memtable_memory_budget) {
  opt->rep.OptimizeUniversalStyleCompaction(memtable_memory_budget);
}

void cabindb_options_set_allow_ingest_behind(
    cabindb_options_t* opt, unsigned char v) {
  opt->rep.allow_ingest_behind = v;
}

unsigned char cabindb_options_get_allow_ingest_behind(cabindb_options_t* opt) {
  return opt->rep.allow_ingest_behind;
}

void cabindb_options_set_compaction_filter(
    cabindb_options_t* opt,
    cabindb_compactionfilter_t* filter) {
  opt->rep.compaction_filter = filter;
}

void cabindb_options_set_compaction_filter_factory(
    cabindb_options_t* opt, cabindb_compactionfilterfactory_t* factory) {
  opt->rep.compaction_filter_factory =
      std::shared_ptr<CompactionFilterFactory>(factory);
}

void cabindb_options_compaction_readahead_size(
    cabindb_options_t* opt, size_t s) {
  opt->rep.compaction_readahead_size = s;
}

size_t cabindb_options_get_compaction_readahead_size(cabindb_options_t* opt) {
  return opt->rep.compaction_readahead_size;
}

void cabindb_options_set_comparator(
    cabindb_options_t* opt,
    cabindb_comparator_t* cmp) {
  opt->rep.comparator = cmp;
}

void cabindb_options_set_merge_operator(
    cabindb_options_t* opt,
    cabindb_mergeoperator_t* merge_operator) {
  opt->rep.merge_operator = std::shared_ptr<MergeOperator>(merge_operator);
}

void cabindb_options_set_create_if_missing(
    cabindb_options_t* opt, unsigned char v) {
  opt->rep.create_if_missing = v;
}

unsigned char cabindb_options_get_create_if_missing(cabindb_options_t* opt) {
  return opt->rep.create_if_missing;
}

void cabindb_options_set_create_missing_column_families(
    cabindb_options_t* opt, unsigned char v) {
  opt->rep.create_missing_column_families = v;
}

unsigned char cabindb_options_get_create_missing_column_families(
    cabindb_options_t* opt) {
  return opt->rep.create_missing_column_families;
}

void cabindb_options_set_error_if_exists(
    cabindb_options_t* opt, unsigned char v) {
  opt->rep.error_if_exists = v;
}

unsigned char cabindb_options_get_error_if_exists(cabindb_options_t* opt) {
  return opt->rep.error_if_exists;
}

void cabindb_options_set_paranoid_checks(
    cabindb_options_t* opt, unsigned char v) {
  opt->rep.paranoid_checks = v;
}

unsigned char cabindb_options_get_paranoid_checks(cabindb_options_t* opt) {
  return opt->rep.paranoid_checks;
}

void cabindb_options_set_db_paths(cabindb_options_t* opt,
                                  const cabindb_dbpath_t** dbpath_values,
                                  size_t num_paths) {
  std::vector<DbPath> db_paths(num_paths);
  for (size_t i = 0; i < num_paths; ++i) {
    db_paths[i] = dbpath_values[i]->rep;
  }
  opt->rep.db_paths = db_paths;
}

void cabindb_options_set_env(cabindb_options_t* opt, cabindb_env_t* env) {
  opt->rep.env = (env ? env->rep : nullptr);
}

void cabindb_options_set_info_log(cabindb_options_t* opt, cabindb_logger_t* l) {
  if (l) {
    opt->rep.info_log = l->rep;
  }
}

void cabindb_options_set_info_log_level(
    cabindb_options_t* opt, int v) {
  opt->rep.info_log_level = static_cast<InfoLogLevel>(v);
}

int cabindb_options_get_info_log_level(cabindb_options_t* opt) {
  return static_cast<int>(opt->rep.info_log_level);
}

void cabindb_options_set_db_write_buffer_size(cabindb_options_t* opt,
                                              size_t s) {
  opt->rep.db_write_buffer_size = s;
}

size_t cabindb_options_get_db_write_buffer_size(cabindb_options_t* opt) {
  return opt->rep.db_write_buffer_size;
}

void cabindb_options_set_write_buffer_size(cabindb_options_t* opt, size_t s) {
  opt->rep.write_buffer_size = s;
}

size_t cabindb_options_get_write_buffer_size(cabindb_options_t* opt) {
  return opt->rep.write_buffer_size;
}

void cabindb_options_set_max_open_files(cabindb_options_t* opt, int n) {
  opt->rep.max_open_files = n;
}

int cabindb_options_get_max_open_files(cabindb_options_t* opt) {
  return opt->rep.max_open_files;
}

void cabindb_options_set_max_file_opening_threads(cabindb_options_t* opt, int n) {
  opt->rep.max_file_opening_threads = n;
}

int cabindb_options_get_max_file_opening_threads(cabindb_options_t* opt) {
  return opt->rep.max_file_opening_threads;
}

void cabindb_options_set_max_total_wal_size(cabindb_options_t* opt, uint64_t n) {
  opt->rep.max_total_wal_size = n;
}

uint64_t cabindb_options_get_max_total_wal_size(cabindb_options_t* opt) {
  return opt->rep.max_total_wal_size;
}

void cabindb_options_set_target_file_size_base(
    cabindb_options_t* opt, uint64_t n) {
  opt->rep.target_file_size_base = n;
}

uint64_t cabindb_options_get_target_file_size_base(cabindb_options_t* opt) {
  return opt->rep.target_file_size_base;
}

void cabindb_options_set_target_file_size_multiplier(
    cabindb_options_t* opt, int n) {
  opt->rep.target_file_size_multiplier = n;
}

int cabindb_options_get_target_file_size_multiplier(cabindb_options_t* opt) {
  return opt->rep.target_file_size_multiplier;
}

void cabindb_options_set_max_bytes_for_level_base(
    cabindb_options_t* opt, uint64_t n) {
  opt->rep.max_bytes_for_level_base = n;
}

uint64_t cabindb_options_get_max_bytes_for_level_base(cabindb_options_t* opt) {
  return opt->rep.max_bytes_for_level_base;
}

void cabindb_options_set_level_compaction_dynamic_level_bytes(
    cabindb_options_t* opt, unsigned char v) {
  opt->rep.level_compaction_dynamic_level_bytes = v;
}

unsigned char cabindb_options_get_level_compaction_dynamic_level_bytes(
    cabindb_options_t* opt) {
  return opt->rep.level_compaction_dynamic_level_bytes;
}

void cabindb_options_set_max_bytes_for_level_multiplier(cabindb_options_t* opt,
                                                        double n) {
  opt->rep.max_bytes_for_level_multiplier = n;
}

double cabindb_options_get_max_bytes_for_level_multiplier(
    cabindb_options_t* opt) {
  return opt->rep.max_bytes_for_level_multiplier;
}

void cabindb_options_set_max_compaction_bytes(cabindb_options_t* opt,
                                              uint64_t n) {
  opt->rep.max_compaction_bytes = n;
}

uint64_t cabindb_options_get_max_compaction_bytes(cabindb_options_t* opt) {
  return opt->rep.max_compaction_bytes;
}

void cabindb_options_set_max_bytes_for_level_multiplier_additional(
    cabindb_options_t* opt, int* level_values, size_t num_levels) {
  opt->rep.max_bytes_for_level_multiplier_additional.resize(num_levels);
  for (size_t i = 0; i < num_levels; ++i) {
    opt->rep.max_bytes_for_level_multiplier_additional[i] = level_values[i];
  }
}

void cabindb_options_enable_statistics(cabindb_options_t* opt) {
  opt->rep.statistics = CABINDB_NAMESPACE::CreateDBStatistics();
}

void cabindb_options_set_skip_stats_update_on_db_open(cabindb_options_t* opt,
                                                      unsigned char val) {
  opt->rep.skip_stats_update_on_db_open = val;
}

unsigned char cabindb_options_get_skip_stats_update_on_db_open(
    cabindb_options_t* opt) {
  return opt->rep.skip_stats_update_on_db_open;
}

void cabindb_options_set_skip_checking_sst_file_sizes_on_db_open(
    cabindb_options_t* opt, unsigned char val) {
  opt->rep.skip_checking_sst_file_sizes_on_db_open = val;
}

unsigned char cabindb_options_get_skip_checking_sst_file_sizes_on_db_open(
    cabindb_options_t* opt) {
  return opt->rep.skip_checking_sst_file_sizes_on_db_open;
}

void cabindb_options_set_num_levels(cabindb_options_t* opt, int n) {
  opt->rep.num_levels = n;
}

int cabindb_options_get_num_levels(cabindb_options_t* opt) {
  return opt->rep.num_levels;
}

void cabindb_options_set_level0_file_num_compaction_trigger(
    cabindb_options_t* opt, int n) {
  opt->rep.level0_file_num_compaction_trigger = n;
}

int cabindb_options_get_level0_file_num_compaction_trigger(
    cabindb_options_t* opt) {
  return opt->rep.level0_file_num_compaction_trigger;
}

void cabindb_options_set_level0_slowdown_writes_trigger(
    cabindb_options_t* opt, int n) {
  opt->rep.level0_slowdown_writes_trigger = n;
}

int cabindb_options_get_level0_slowdown_writes_trigger(cabindb_options_t* opt) {
  return opt->rep.level0_slowdown_writes_trigger;
}

void cabindb_options_set_level0_stop_writes_trigger(
    cabindb_options_t* opt, int n) {
  opt->rep.level0_stop_writes_trigger = n;
}

int cabindb_options_get_level0_stop_writes_trigger(cabindb_options_t* opt) {
  return opt->rep.level0_stop_writes_trigger;
}

void cabindb_options_set_max_mem_compaction_level(cabindb_options_t* /*opt*/,
                                                  int /*n*/) {}

void cabindb_options_set_wal_recovery_mode(cabindb_options_t* opt,int mode) {
  opt->rep.wal_recovery_mode = static_cast<WALRecoveryMode>(mode);
}

int cabindb_options_get_wal_recovery_mode(cabindb_options_t* opt) {
  return static_cast<int>(opt->rep.wal_recovery_mode);
}

void cabindb_options_set_compression(cabindb_options_t* opt, int t) {
  opt->rep.compression = static_cast<CompressionType>(t);
}

int cabindb_options_get_compression(cabindb_options_t* opt) {
  return opt->rep.compression;
}

void cabindb_options_set_bottommost_compression(cabindb_options_t* opt, int t) {
  opt->rep.bottommost_compression = static_cast<CompressionType>(t);
}

int cabindb_options_get_bottommost_compression(cabindb_options_t* opt) {
  return opt->rep.bottommost_compression;
}

void cabindb_options_set_compression_per_level(cabindb_options_t* opt,
                                               int* level_values,
                                               size_t num_levels) {
  opt->rep.compression_per_level.resize(num_levels);
  for (size_t i = 0; i < num_levels; ++i) {
    opt->rep.compression_per_level[i] =
      static_cast<CompressionType>(level_values[i]);
  }
}

void cabindb_options_set_bottommost_compression_options(cabindb_options_t* opt,
                                                        int w_bits, int level,
                                                        int strategy,
                                                        int max_dict_bytes,
                                                        unsigned char enabled) {
  opt->rep.bottommost_compression_opts.window_bits = w_bits;
  opt->rep.bottommost_compression_opts.level = level;
  opt->rep.bottommost_compression_opts.strategy = strategy;
  opt->rep.bottommost_compression_opts.max_dict_bytes = max_dict_bytes;
  opt->rep.bottommost_compression_opts.enabled = enabled;
}

void cabindb_options_set_bottommost_compression_options_zstd_max_train_bytes(
    cabindb_options_t* opt, int zstd_max_train_bytes, unsigned char enabled) {
  opt->rep.bottommost_compression_opts.zstd_max_train_bytes =
      zstd_max_train_bytes;
  opt->rep.bottommost_compression_opts.enabled = enabled;
}

void cabindb_options_set_compression_options(cabindb_options_t* opt, int w_bits,
                                             int level, int strategy,
                                             int max_dict_bytes) {
  opt->rep.compression_opts.window_bits = w_bits;
  opt->rep.compression_opts.level = level;
  opt->rep.compression_opts.strategy = strategy;
  opt->rep.compression_opts.max_dict_bytes = max_dict_bytes;
}

void cabindb_options_set_compression_options_zstd_max_train_bytes(
    cabindb_options_t* opt, int zstd_max_train_bytes) {
  opt->rep.compression_opts.zstd_max_train_bytes = zstd_max_train_bytes;
}

void cabindb_options_set_prefix_extractor(
    cabindb_options_t* opt, cabindb_slicetransform_t* prefix_extractor) {
  opt->rep.prefix_extractor.reset(prefix_extractor);
}

void cabindb_options_set_use_fsync(
    cabindb_options_t* opt, int use_fsync) {
  opt->rep.use_fsync = use_fsync;
}

int cabindb_options_get_use_fsync(cabindb_options_t* opt) {
  return opt->rep.use_fsync;
}

void cabindb_options_set_db_log_dir(
    cabindb_options_t* opt, const char* db_log_dir) {
  opt->rep.db_log_dir = db_log_dir;
}

void cabindb_options_set_wal_dir(
    cabindb_options_t* opt, const char* v) {
  opt->rep.wal_dir = v;
}

void cabindb_options_set_WAL_ttl_seconds(cabindb_options_t* opt, uint64_t ttl) {
  opt->rep.WAL_ttl_seconds = ttl;
}

uint64_t cabindb_options_get_WAL_ttl_seconds(cabindb_options_t* opt) {
  return opt->rep.WAL_ttl_seconds;
}

void cabindb_options_set_WAL_size_limit_MB(
    cabindb_options_t* opt, uint64_t limit) {
  opt->rep.WAL_size_limit_MB = limit;
}

uint64_t cabindb_options_get_WAL_size_limit_MB(cabindb_options_t* opt) {
  return opt->rep.WAL_size_limit_MB;
}

void cabindb_options_set_manifest_preallocation_size(
    cabindb_options_t* opt, size_t v) {
  opt->rep.manifest_preallocation_size = v;
}

size_t cabindb_options_get_manifest_preallocation_size(cabindb_options_t* opt) {
  return opt->rep.manifest_preallocation_size;
}

// noop
void cabindb_options_set_purge_redundant_kvs_while_flush(
    cabindb_options_t* /*opt*/, unsigned char /*v*/) {}

void cabindb_options_set_use_direct_reads(cabindb_options_t* opt,
                                          unsigned char v) {
  opt->rep.use_direct_reads = v;
}

unsigned char cabindb_options_get_use_direct_reads(cabindb_options_t* opt) {
  return opt->rep.use_direct_reads;
}

void cabindb_options_set_use_direct_io_for_flush_and_compaction(
    cabindb_options_t* opt, unsigned char v) {
  opt->rep.use_direct_io_for_flush_and_compaction = v;
}

unsigned char cabindb_options_get_use_direct_io_for_flush_and_compaction(
    cabindb_options_t* opt) {
  return opt->rep.use_direct_io_for_flush_and_compaction;
}

void cabindb_options_set_allow_mmap_reads(
    cabindb_options_t* opt, unsigned char v) {
  opt->rep.allow_mmap_reads = v;
}

unsigned char cabindb_options_get_allow_mmap_reads(cabindb_options_t* opt) {
  return opt->rep.allow_mmap_reads;
}

void cabindb_options_set_allow_mmap_writes(
    cabindb_options_t* opt, unsigned char v) {
  opt->rep.allow_mmap_writes = v;
}

unsigned char cabindb_options_get_allow_mmap_writes(cabindb_options_t* opt) {
  return opt->rep.allow_mmap_writes;
}

void cabindb_options_set_is_fd_close_on_exec(
    cabindb_options_t* opt, unsigned char v) {
  opt->rep.is_fd_close_on_exec = v;
}

unsigned char cabindb_options_get_is_fd_close_on_exec(cabindb_options_t* opt) {
  return opt->rep.is_fd_close_on_exec;
}

void cabindb_options_set_skip_log_error_on_recovery(
    cabindb_options_t* opt, unsigned char v) {
  opt->rep.skip_log_error_on_recovery = v;
}

unsigned char cabindb_options_get_skip_log_error_on_recovery(
    cabindb_options_t* opt) {
  return opt->rep.skip_log_error_on_recovery;
}

void cabindb_options_set_stats_dump_period_sec(
    cabindb_options_t* opt, unsigned int v) {
  opt->rep.stats_dump_period_sec = v;
}

unsigned int cabindb_options_get_stats_dump_period_sec(cabindb_options_t* opt) {
  return opt->rep.stats_dump_period_sec;
}

void cabindb_options_set_stats_persist_period_sec(cabindb_options_t* opt,
                                                  unsigned int v) {
  opt->rep.stats_persist_period_sec = v;
}

unsigned int cabindb_options_get_stats_persist_period_sec(
    cabindb_options_t* opt) {
  return opt->rep.stats_persist_period_sec;
}

void cabindb_options_set_advise_random_on_open(
    cabindb_options_t* opt, unsigned char v) {
  opt->rep.advise_random_on_open = v;
}

unsigned char cabindb_options_get_advise_random_on_open(
    cabindb_options_t* opt) {
  return opt->rep.advise_random_on_open;
}

void cabindb_options_set_access_hint_on_compaction_start(
    cabindb_options_t* opt, int v) {
  switch(v) {
    case 0:
      opt->rep.access_hint_on_compaction_start =
          CABINDB_NAMESPACE::Options::NONE;
      break;
    case 1:
      opt->rep.access_hint_on_compaction_start =
          CABINDB_NAMESPACE::Options::NORMAL;
      break;
    case 2:
      opt->rep.access_hint_on_compaction_start =
          CABINDB_NAMESPACE::Options::SEQUENTIAL;
      break;
    case 3:
      opt->rep.access_hint_on_compaction_start =
          CABINDB_NAMESPACE::Options::WILLNEED;
      break;
  }
}

int cabindb_options_get_access_hint_on_compaction_start(
    cabindb_options_t* opt) {
  return opt->rep.access_hint_on_compaction_start;
}

void cabindb_options_set_use_adaptive_mutex(
    cabindb_options_t* opt, unsigned char v) {
  opt->rep.use_adaptive_mutex = v;
}

unsigned char cabindb_options_get_use_adaptive_mutex(cabindb_options_t* opt) {
  return opt->rep.use_adaptive_mutex;
}

void cabindb_options_set_wal_bytes_per_sync(
    cabindb_options_t* opt, uint64_t v) {
  opt->rep.wal_bytes_per_sync = v;
}

uint64_t cabindb_options_get_wal_bytes_per_sync(cabindb_options_t* opt) {
  return opt->rep.wal_bytes_per_sync;
}

void cabindb_options_set_bytes_per_sync(
    cabindb_options_t* opt, uint64_t v) {
  opt->rep.bytes_per_sync = v;
}

uint64_t cabindb_options_get_bytes_per_sync(cabindb_options_t* opt) {
  return opt->rep.bytes_per_sync;
}

void cabindb_options_set_writable_file_max_buffer_size(cabindb_options_t* opt,
                                                       uint64_t v) {
  opt->rep.writable_file_max_buffer_size = static_cast<size_t>(v);
}

uint64_t cabindb_options_get_writable_file_max_buffer_size(
    cabindb_options_t* opt) {
  return opt->rep.writable_file_max_buffer_size;
}

void cabindb_options_set_allow_concurrent_memtable_write(cabindb_options_t* opt,
                                                         unsigned char v) {
  opt->rep.allow_concurrent_memtable_write = v;
}

unsigned char cabindb_options_get_allow_concurrent_memtable_write(
    cabindb_options_t* opt) {
  return opt->rep.allow_concurrent_memtable_write;
}

void cabindb_options_set_enable_write_thread_adaptive_yield(
    cabindb_options_t* opt, unsigned char v) {
  opt->rep.enable_write_thread_adaptive_yield = v;
}

unsigned char cabindb_options_get_enable_write_thread_adaptive_yield(
    cabindb_options_t* opt) {
  return opt->rep.enable_write_thread_adaptive_yield;
}

void cabindb_options_set_max_sequential_skip_in_iterations(
    cabindb_options_t* opt, uint64_t v) {
  opt->rep.max_sequential_skip_in_iterations = v;
}

uint64_t cabindb_options_get_max_sequential_skip_in_iterations(
    cabindb_options_t* opt) {
  return opt->rep.max_sequential_skip_in_iterations;
}

void cabindb_options_set_max_write_buffer_number(cabindb_options_t* opt, int n) {
  opt->rep.max_write_buffer_number = n;
}

int cabindb_options_get_max_write_buffer_number(cabindb_options_t* opt) {
  return opt->rep.max_write_buffer_number;
}

void cabindb_options_set_min_write_buffer_number_to_merge(cabindb_options_t* opt, int n) {
  opt->rep.min_write_buffer_number_to_merge = n;
}

int cabindb_options_get_min_write_buffer_number_to_merge(
    cabindb_options_t* opt) {
  return opt->rep.min_write_buffer_number_to_merge;
}

void cabindb_options_set_max_write_buffer_number_to_maintain(
    cabindb_options_t* opt, int n) {
  opt->rep.max_write_buffer_number_to_maintain = n;
}

int cabindb_options_get_max_write_buffer_number_to_maintain(
    cabindb_options_t* opt) {
  return opt->rep.max_write_buffer_number_to_maintain;
}

void cabindb_options_set_max_write_buffer_size_to_maintain(
    cabindb_options_t* opt, int64_t n) {
  opt->rep.max_write_buffer_size_to_maintain = n;
}

int64_t cabindb_options_get_max_write_buffer_size_to_maintain(
    cabindb_options_t* opt) {
  return opt->rep.max_write_buffer_size_to_maintain;
}

void cabindb_options_set_enable_pipelined_write(cabindb_options_t* opt,
                                                unsigned char v) {
  opt->rep.enable_pipelined_write = v;
}

unsigned char cabindb_options_get_enable_pipelined_write(
    cabindb_options_t* opt) {
  return opt->rep.enable_pipelined_write;
}

void cabindb_options_set_unordered_write(cabindb_options_t* opt,
                                         unsigned char v) {
  opt->rep.unordered_write = v;
}

unsigned char cabindb_options_get_unordered_write(cabindb_options_t* opt) {
  return opt->rep.unordered_write;
}

void cabindb_options_set_max_subcompactions(cabindb_options_t* opt,
                                            uint32_t n) {
  opt->rep.max_subcompactions = n;
}

uint32_t cabindb_options_get_max_subcompactions(cabindb_options_t* opt) {
  return opt->rep.max_subcompactions;
}

void cabindb_options_set_max_background_jobs(cabindb_options_t* opt, int n) {
  opt->rep.max_background_jobs = n;
}

int cabindb_options_get_max_background_jobs(cabindb_options_t* opt) {
  return opt->rep.max_background_jobs;
}

void cabindb_options_set_max_background_compactions(cabindb_options_t* opt, int n) {
  opt->rep.max_background_compactions = n;
}

int cabindb_options_get_max_background_compactions(cabindb_options_t* opt) {
  return opt->rep.max_background_compactions;
}

void cabindb_options_set_base_background_compactions(cabindb_options_t* opt,
                                                     int n) {
  opt->rep.base_background_compactions = n;
}

int cabindb_options_get_base_background_compactions(cabindb_options_t* opt) {
  return opt->rep.base_background_compactions;
}

void cabindb_options_set_max_background_flushes(cabindb_options_t* opt, int n) {
  opt->rep.max_background_flushes = n;
}

int cabindb_options_get_max_background_flushes(cabindb_options_t* opt) {
  return opt->rep.max_background_flushes;
}

void cabindb_options_set_max_log_file_size(cabindb_options_t* opt, size_t v) {
  opt->rep.max_log_file_size = v;
}

size_t cabindb_options_get_max_log_file_size(cabindb_options_t* opt) {
  return opt->rep.max_log_file_size;
}

void cabindb_options_set_log_file_time_to_roll(cabindb_options_t* opt, size_t v) {
  opt->rep.log_file_time_to_roll = v;
}

size_t cabindb_options_get_log_file_time_to_roll(cabindb_options_t* opt) {
  return opt->rep.log_file_time_to_roll;
}

void cabindb_options_set_keep_log_file_num(cabindb_options_t* opt, size_t v) {
  opt->rep.keep_log_file_num = v;
}

size_t cabindb_options_get_keep_log_file_num(cabindb_options_t* opt) {
  return opt->rep.keep_log_file_num;
}

void cabindb_options_set_recycle_log_file_num(cabindb_options_t* opt,
                                              size_t v) {
  opt->rep.recycle_log_file_num = v;
}

size_t cabindb_options_get_recycle_log_file_num(cabindb_options_t* opt) {
  return opt->rep.recycle_log_file_num;
}

void cabindb_options_set_soft_rate_limit(cabindb_options_t* opt, double v) {
  opt->rep.soft_rate_limit = v;
}

double cabindb_options_get_soft_rate_limit(cabindb_options_t* opt) {
  return opt->rep.soft_rate_limit;
}

void cabindb_options_set_hard_rate_limit(cabindb_options_t* opt, double v) {
  opt->rep.hard_rate_limit = v;
}

double cabindb_options_get_hard_rate_limit(cabindb_options_t* opt) {
  return opt->rep.hard_rate_limit;
}

void cabindb_options_set_soft_pending_compaction_bytes_limit(cabindb_options_t* opt, size_t v) {
  opt->rep.soft_pending_compaction_bytes_limit = v;
}

size_t cabindb_options_get_soft_pending_compaction_bytes_limit(
    cabindb_options_t* opt) {
  return opt->rep.soft_pending_compaction_bytes_limit;
}

void cabindb_options_set_hard_pending_compaction_bytes_limit(cabindb_options_t* opt, size_t v) {
  opt->rep.hard_pending_compaction_bytes_limit = v;
}

size_t cabindb_options_get_hard_pending_compaction_bytes_limit(
    cabindb_options_t* opt) {
  return opt->rep.hard_pending_compaction_bytes_limit;
}

void cabindb_options_set_rate_limit_delay_max_milliseconds(
    cabindb_options_t* opt, unsigned int v) {
  opt->rep.rate_limit_delay_max_milliseconds = v;
}

unsigned int cabindb_options_get_rate_limit_delay_max_milliseconds(
    cabindb_options_t* opt) {
  return opt->rep.rate_limit_delay_max_milliseconds;
}

void cabindb_options_set_max_manifest_file_size(
    cabindb_options_t* opt, size_t v) {
  opt->rep.max_manifest_file_size = v;
}

size_t cabindb_options_get_max_manifest_file_size(cabindb_options_t* opt) {
  return opt->rep.max_manifest_file_size;
}

void cabindb_options_set_table_cache_numshardbits(
    cabindb_options_t* opt, int v) {
  opt->rep.table_cache_numshardbits = v;
}

int cabindb_options_get_table_cache_numshardbits(cabindb_options_t* opt) {
  return opt->rep.table_cache_numshardbits;
}

void cabindb_options_set_table_cache_remove_scan_count_limit(
    cabindb_options_t* /*opt*/, int /*v*/) {
  // this option is deprecated
}

void cabindb_options_set_arena_block_size(
    cabindb_options_t* opt, size_t v) {
  opt->rep.arena_block_size = v;
}

size_t cabindb_options_get_arena_block_size(cabindb_options_t* opt) {
  return opt->rep.arena_block_size;
}

void cabindb_options_set_disable_auto_compactions(cabindb_options_t* opt, int disable) {
  opt->rep.disable_auto_compactions = disable;
}

unsigned char cabindb_options_get_disable_auto_compactions(
    cabindb_options_t* opt) {
  return opt->rep.disable_auto_compactions;
}

void cabindb_options_set_optimize_filters_for_hits(cabindb_options_t* opt, int v) {
  opt->rep.optimize_filters_for_hits = v;
}

unsigned char cabindb_options_get_optimize_filters_for_hits(
    cabindb_options_t* opt) {
  return opt->rep.optimize_filters_for_hits;
}

void cabindb_options_set_delete_obsolete_files_period_micros(
    cabindb_options_t* opt, uint64_t v) {
  opt->rep.delete_obsolete_files_period_micros = v;
}

uint64_t cabindb_options_get_delete_obsolete_files_period_micros(
    cabindb_options_t* opt) {
  return opt->rep.delete_obsolete_files_period_micros;
}

void cabindb_options_prepare_for_bulk_load(cabindb_options_t* opt) {
  opt->rep.PrepareForBulkLoad();
}

void cabindb_options_set_memtable_vector_rep(cabindb_options_t *opt) {
  opt->rep.memtable_factory.reset(new CABINDB_NAMESPACE::VectorRepFactory);
}

void cabindb_options_set_memtable_prefix_bloom_size_ratio(
    cabindb_options_t* opt, double v) {
  opt->rep.memtable_prefix_bloom_size_ratio = v;
}

double cabindb_options_get_memtable_prefix_bloom_size_ratio(
    cabindb_options_t* opt) {
  return opt->rep.memtable_prefix_bloom_size_ratio;
}

void cabindb_options_set_memtable_huge_page_size(cabindb_options_t* opt,
                                                 size_t v) {
  opt->rep.memtable_huge_page_size = v;
}

size_t cabindb_options_get_memtable_huge_page_size(cabindb_options_t* opt) {
  return opt->rep.memtable_huge_page_size;
}

void cabindb_options_set_hash_skip_list_rep(
    cabindb_options_t *opt, size_t bucket_count,
    int32_t skiplist_height, int32_t skiplist_branching_factor) {
  CABINDB_NAMESPACE::MemTableRepFactory* factory =
      CABINDB_NAMESPACE::NewHashSkipListRepFactory(
          bucket_count, skiplist_height, skiplist_branching_factor);
  opt->rep.memtable_factory.reset(factory);
}

void cabindb_options_set_hash_link_list_rep(
    cabindb_options_t *opt, size_t bucket_count) {
  opt->rep.memtable_factory.reset(
      CABINDB_NAMESPACE::NewHashLinkListRepFactory(bucket_count));
}

void cabindb_options_set_plain_table_factory(
    cabindb_options_t *opt, uint32_t user_key_len, int bloom_bits_per_key,
    double hash_table_ratio, size_t index_sparseness) {
  CABINDB_NAMESPACE::PlainTableOptions options;
  options.user_key_len = user_key_len;
  options.bloom_bits_per_key = bloom_bits_per_key;
  options.hash_table_ratio = hash_table_ratio;
  options.index_sparseness = index_sparseness;

  CABINDB_NAMESPACE::TableFactory* factory =
      CABINDB_NAMESPACE::NewPlainTableFactory(options);
  opt->rep.table_factory.reset(factory);
}

void cabindb_options_set_max_successive_merges(
    cabindb_options_t* opt, size_t v) {
  opt->rep.max_successive_merges = v;
}

size_t cabindb_options_get_max_successive_merges(cabindb_options_t* opt) {
  return opt->rep.max_successive_merges;
}

void cabindb_options_set_bloom_locality(
    cabindb_options_t* opt, uint32_t v) {
  opt->rep.bloom_locality = v;
}

uint32_t cabindb_options_get_bloom_locality(cabindb_options_t* opt) {
  return opt->rep.bloom_locality;
}

void cabindb_options_set_inplace_update_support(
    cabindb_options_t* opt, unsigned char v) {
  opt->rep.inplace_update_support = v;
}

unsigned char cabindb_options_get_inplace_update_support(
    cabindb_options_t* opt) {
  return opt->rep.inplace_update_support;
}

void cabindb_options_set_inplace_update_num_locks(
    cabindb_options_t* opt, size_t v) {
  opt->rep.inplace_update_num_locks = v;
}

size_t cabindb_options_get_inplace_update_num_locks(cabindb_options_t* opt) {
  return opt->rep.inplace_update_num_locks;
}

void cabindb_options_set_report_bg_io_stats(
    cabindb_options_t* opt, int v) {
  opt->rep.report_bg_io_stats = v;
}

unsigned char cabindb_options_get_report_bg_io_stats(cabindb_options_t* opt) {
  return opt->rep.report_bg_io_stats;
}

void cabindb_options_set_compaction_style(cabindb_options_t *opt, int style) {
  opt->rep.compaction_style =
      static_cast<CABINDB_NAMESPACE::CompactionStyle>(style);
}

int cabindb_options_get_compaction_style(cabindb_options_t* opt) {
  return opt->rep.compaction_style;
}

void cabindb_options_set_universal_compaction_options(cabindb_options_t *opt, cabindb_universal_compaction_options_t *uco) {
  opt->rep.compaction_options_universal = *(uco->rep);
}

void cabindb_options_set_fifo_compaction_options(
    cabindb_options_t* opt,
    cabindb_fifo_compaction_options_t* fifo) {
  opt->rep.compaction_options_fifo = fifo->rep;
}

char *cabindb_options_statistics_get_string(cabindb_options_t *opt) {
  CABINDB_NAMESPACE::Statistics* statistics = opt->rep.statistics.get();
  if (statistics) {
    return strdup(statistics->ToString().c_str());
  }
  return nullptr;
}

void cabindb_options_set_ratelimiter(cabindb_options_t *opt, cabindb_ratelimiter_t *limiter) {
  if (limiter) {
    opt->rep.rate_limiter = limiter->rep;
  }
}

void cabindb_options_set_atomic_flush(cabindb_options_t* opt,
                                      unsigned char atomic_flush) {
  opt->rep.atomic_flush = atomic_flush;
}

unsigned char cabindb_options_get_atomic_flush(cabindb_options_t* opt) {
  return opt->rep.atomic_flush;
}

cabindb_ratelimiter_t* cabindb_ratelimiter_create(
    int64_t rate_bytes_per_sec,
    int64_t refill_period_us,
    int32_t fairness) {
  cabindb_ratelimiter_t* rate_limiter = new cabindb_ratelimiter_t;
  rate_limiter->rep.reset(
               NewGenericRateLimiter(rate_bytes_per_sec,
                                     refill_period_us, fairness));
  return rate_limiter;
}

void cabindb_ratelimiter_destroy(cabindb_ratelimiter_t *limiter) {
  delete limiter;
}

void cabindb_options_set_row_cache(cabindb_options_t* opt, cabindb_cache_t* cache) {
  if(cache) {
    opt->rep.row_cache = cache->rep;
  }
}

void cabindb_set_perf_level(int v) {
  PerfLevel level = static_cast<PerfLevel>(v);
  SetPerfLevel(level);
}

cabindb_perfcontext_t* cabindb_perfcontext_create() {
  cabindb_perfcontext_t* context = new cabindb_perfcontext_t;
  context->rep = CABINDB_NAMESPACE::get_perf_context();
  return context;
}

void cabindb_perfcontext_reset(cabindb_perfcontext_t* context) {
  context->rep->Reset();
}

char* cabindb_perfcontext_report(cabindb_perfcontext_t* context,
    unsigned char exclude_zero_counters) {
  return strdup(context->rep->ToString(exclude_zero_counters).c_str());
}

uint64_t cabindb_perfcontext_metric(cabindb_perfcontext_t* context,
    int metric) {
  PerfContext* rep = context->rep;
  switch (metric) {
    case cabindb_user_key_comparison_count:
      return rep->user_key_comparison_count;
    case cabindb_block_cache_hit_count:
      return rep->block_cache_hit_count;
    case cabindb_block_read_count:
      return rep->block_read_count;
    case cabindb_block_read_byte:
      return rep->block_read_byte;
    case cabindb_block_read_time:
      return rep->block_read_time;
    case cabindb_block_checksum_time:
      return rep->block_checksum_time;
    case cabindb_block_decompress_time:
      return rep->block_decompress_time;
    case cabindb_get_read_bytes:
      return rep->get_read_bytes;
    case cabindb_multiget_read_bytes:
      return rep->multiget_read_bytes;
    case cabindb_iter_read_bytes:
      return rep->iter_read_bytes;
    case cabindb_internal_key_skipped_count:
      return rep->internal_key_skipped_count;
    case cabindb_internal_delete_skipped_count:
      return rep->internal_delete_skipped_count;
    case cabindb_internal_recent_skipped_count:
      return rep->internal_recent_skipped_count;
    case cabindb_internal_merge_count:
      return rep->internal_merge_count;
    case cabindb_get_snapshot_time:
      return rep->get_snapshot_time;
    case cabindb_get_from_memtable_time:
      return rep->get_from_memtable_time;
    case cabindb_get_from_memtable_count:
      return rep->get_from_memtable_count;
    case cabindb_get_post_process_time:
      return rep->get_post_process_time;
    case cabindb_get_from_output_files_time:
      return rep->get_from_output_files_time;
    case cabindb_seek_on_memtable_time:
      return rep->seek_on_memtable_time;
    case cabindb_seek_on_memtable_count:
      return rep->seek_on_memtable_count;
    case cabindb_next_on_memtable_count:
      return rep->next_on_memtable_count;
    case cabindb_prev_on_memtable_count:
      return rep->prev_on_memtable_count;
    case cabindb_seek_child_seek_time:
      return rep->seek_child_seek_time;
    case cabindb_seek_child_seek_count:
      return rep->seek_child_seek_count;
    case cabindb_seek_min_heap_time:
      return rep->seek_min_heap_time;
    case cabindb_seek_max_heap_time:
      return rep->seek_max_heap_time;
    case cabindb_seek_internal_seek_time:
      return rep->seek_internal_seek_time;
    case cabindb_find_next_user_entry_time:
      return rep->find_next_user_entry_time;
    case cabindb_write_wal_time:
      return rep->write_wal_time;
    case cabindb_write_memtable_time:
      return rep->write_memtable_time;
    case cabindb_write_delay_time:
      return rep->write_delay_time;
    case cabindb_write_pre_and_post_process_time:
      return rep->write_pre_and_post_process_time;
    case cabindb_db_mutex_lock_nanos:
      return rep->db_mutex_lock_nanos;
    case cabindb_db_condition_wait_nanos:
      return rep->db_condition_wait_nanos;
    case cabindb_merge_operator_time_nanos:
      return rep->merge_operator_time_nanos;
    case cabindb_read_index_block_nanos:
      return rep->read_index_block_nanos;
    case cabindb_read_filter_block_nanos:
      return rep->read_filter_block_nanos;
    case cabindb_new_table_block_iter_nanos:
      return rep->new_table_block_iter_nanos;
    case cabindb_new_table_iterator_nanos:
      return rep->new_table_iterator_nanos;
    case cabindb_block_seek_nanos:
      return rep->block_seek_nanos;
    case cabindb_find_table_nanos:
      return rep->find_table_nanos;
    case cabindb_bloom_memtable_hit_count:
      return rep->bloom_memtable_hit_count;
    case cabindb_bloom_memtable_miss_count:
      return rep->bloom_memtable_miss_count;
    case cabindb_bloom_sst_hit_count:
      return rep->bloom_sst_hit_count;
    case cabindb_bloom_sst_miss_count:
      return rep->bloom_sst_miss_count;
    case cabindb_key_lock_wait_time:
      return rep->key_lock_wait_time;
    case cabindb_key_lock_wait_count:
      return rep->key_lock_wait_count;
    case cabindb_env_new_sequential_file_nanos:
      return rep->env_new_sequential_file_nanos;
    case cabindb_env_new_random_access_file_nanos:
      return rep->env_new_random_access_file_nanos;
    case cabindb_env_new_writable_file_nanos:
      return rep->env_new_writable_file_nanos;
    case cabindb_env_reuse_writable_file_nanos:
      return rep->env_reuse_writable_file_nanos;
    case cabindb_env_new_random_rw_file_nanos:
      return rep->env_new_random_rw_file_nanos;
    case cabindb_env_new_directory_nanos:
      return rep->env_new_directory_nanos;
    case cabindb_env_file_exists_nanos:
      return rep->env_file_exists_nanos;
    case cabindb_env_get_children_nanos:
      return rep->env_get_children_nanos;
    case cabindb_env_get_children_file_attributes_nanos:
      return rep->env_get_children_file_attributes_nanos;
    case cabindb_env_delete_file_nanos:
      return rep->env_delete_file_nanos;
    case cabindb_env_create_dir_nanos:
      return rep->env_create_dir_nanos;
    case cabindb_env_create_dir_if_missing_nanos:
      return rep->env_create_dir_if_missing_nanos;
    case cabindb_env_delete_dir_nanos:
      return rep->env_delete_dir_nanos;
    case cabindb_env_get_file_size_nanos:
      return rep->env_get_file_size_nanos;
    case cabindb_env_get_file_modification_time_nanos:
      return rep->env_get_file_modification_time_nanos;
    case cabindb_env_rename_file_nanos:
      return rep->env_rename_file_nanos;
    case cabindb_env_link_file_nanos:
      return rep->env_link_file_nanos;
    case cabindb_env_lock_file_nanos:
      return rep->env_lock_file_nanos;
    case cabindb_env_unlock_file_nanos:
      return rep->env_unlock_file_nanos;
    case cabindb_env_new_logger_nanos:
      return rep->env_new_logger_nanos;
    default:
      break;
  }
  return 0;
}

void cabindb_perfcontext_destroy(cabindb_perfcontext_t* context) {
  delete context;
}

/*
TODO:
DB::OpenForReadOnly
DB::KeyMayExist
DB::GetOptions
DB::GetSortedWalFiles
DB::GetLatestSequenceNumber
DB::GetUpdatesSince
DB::GetDbIdentity
DB::RunManualCompaction
custom cache
table_properties_collectors
*/

cabindb_compactionfilter_t* cabindb_compactionfilter_create(
    void* state,
    void (*destructor)(void*),
    unsigned char (*filter)(
        void*,
        int level,
        const char* key, size_t key_length,
        const char* existing_value, size_t value_length,
        char** new_value, size_t *new_value_length,
        unsigned char* value_changed),
    const char* (*name)(void*)) {
  cabindb_compactionfilter_t* result = new cabindb_compactionfilter_t;
  result->state_ = state;
  result->destructor_ = destructor;
  result->filter_ = filter;
  result->ignore_snapshots_ = true;
  result->name_ = name;
  return result;
}

void cabindb_compactionfilter_set_ignore_snapshots(
  cabindb_compactionfilter_t* filter,
  unsigned char whether_ignore) {
  filter->ignore_snapshots_ = whether_ignore;
}

void cabindb_compactionfilter_destroy(cabindb_compactionfilter_t* filter) {
  delete filter;
}

unsigned char cabindb_compactionfiltercontext_is_full_compaction(
    cabindb_compactionfiltercontext_t* context) {
  return context->rep.is_full_compaction;
}

unsigned char cabindb_compactionfiltercontext_is_manual_compaction(
    cabindb_compactionfiltercontext_t* context) {
  return context->rep.is_manual_compaction;
}

cabindb_compactionfilterfactory_t* cabindb_compactionfilterfactory_create(
    void* state, void (*destructor)(void*),
    cabindb_compactionfilter_t* (*create_compaction_filter)(
        void*, cabindb_compactionfiltercontext_t* context),
    const char* (*name)(void*)) {
  cabindb_compactionfilterfactory_t* result =
      new cabindb_compactionfilterfactory_t;
  result->state_ = state;
  result->destructor_ = destructor;
  result->create_compaction_filter_ = create_compaction_filter;
  result->name_ = name;
  return result;
}

void cabindb_compactionfilterfactory_destroy(
    cabindb_compactionfilterfactory_t* factory) {
  delete factory;
}

cabindb_comparator_t* cabindb_comparator_create(
    void* state,
    void (*destructor)(void*),
    int (*compare)(
        void*,
        const char* a, size_t alen,
        const char* b, size_t blen),
    const char* (*name)(void*)) {
  cabindb_comparator_t* result = new cabindb_comparator_t;
  result->state_ = state;
  result->destructor_ = destructor;
  result->compare_ = compare;
  result->name_ = name;
  return result;
}

void cabindb_comparator_destroy(cabindb_comparator_t* cmp) {
  delete cmp;
}

cabindb_filterpolicy_t* cabindb_filterpolicy_create(
    void* state,
    void (*destructor)(void*),
    char* (*create_filter)(
        void*,
        const char* const* key_array, const size_t* key_length_array,
        int num_keys,
        size_t* filter_length),
    unsigned char (*key_may_match)(
        void*,
        const char* key, size_t length,
        const char* filter, size_t filter_length),
    void (*delete_filter)(
        void*,
        const char* filter, size_t filter_length),
    const char* (*name)(void*)) {
  cabindb_filterpolicy_t* result = new cabindb_filterpolicy_t;
  result->state_ = state;
  result->destructor_ = destructor;
  result->create_ = create_filter;
  result->key_match_ = key_may_match;
  result->delete_filter_ = delete_filter;
  result->name_ = name;
  return result;
}

void cabindb_filterpolicy_destroy(cabindb_filterpolicy_t* filter) {
  delete filter;
}

cabindb_filterpolicy_t* cabindb_filterpolicy_create_bloom_format(int bits_per_key, bool original_format) {
  // Make a cabindb_filterpolicy_t, but override all of its methods so
  // they delegate to a NewBloomFilterPolicy() instead of user
  // supplied C functions.
  struct Wrapper : public cabindb_filterpolicy_t {
    const FilterPolicy* rep_;
    ~Wrapper() override { delete rep_; }
    const char* Name() const override { return rep_->Name(); }
    void CreateFilter(const Slice* keys, int n,
                      std::string* dst) const override {
      return rep_->CreateFilter(keys, n, dst);
    }
    bool KeyMayMatch(const Slice& key, const Slice& filter) const override {
      return rep_->KeyMayMatch(key, filter);
    }
    // No need to override GetFilterBitsBuilder if this one is overridden
    CABINDB_NAMESPACE::FilterBitsBuilder* GetBuilderWithContext(
        const CABINDB_NAMESPACE::FilterBuildingContext& context)
        const override {
      return rep_->GetBuilderWithContext(context);
    }
    CABINDB_NAMESPACE::FilterBitsReader* GetFilterBitsReader(
        const Slice& contents) const override {
      return rep_->GetFilterBitsReader(contents);
    }
    static void DoNothing(void*) {}
  };
  Wrapper* wrapper = new Wrapper;
  wrapper->rep_ = NewBloomFilterPolicy(bits_per_key, original_format);
  wrapper->state_ = nullptr;
  wrapper->delete_filter_ = nullptr;
  wrapper->destructor_ = &Wrapper::DoNothing;
  return wrapper;
}

cabindb_filterpolicy_t* cabindb_filterpolicy_create_bloom_full(int bits_per_key) {
  return cabindb_filterpolicy_create_bloom_format(bits_per_key, false);
}

cabindb_filterpolicy_t* cabindb_filterpolicy_create_bloom(int bits_per_key) {
  return cabindb_filterpolicy_create_bloom_format(bits_per_key, true);
}

cabindb_mergeoperator_t* cabindb_mergeoperator_create(
    void* state, void (*destructor)(void*),
    char* (*full_merge)(void*, const char* key, size_t key_length,
                        const char* existing_value,
                        size_t existing_value_length,
                        const char* const* operands_list,
                        const size_t* operands_list_length, int num_operands,
                        unsigned char* success, size_t* new_value_length),
    char* (*partial_merge)(void*, const char* key, size_t key_length,
                           const char* const* operands_list,
                           const size_t* operands_list_length, int num_operands,
                           unsigned char* success, size_t* new_value_length),
    void (*delete_value)(void*, const char* value, size_t value_length),
    const char* (*name)(void*)) {
  cabindb_mergeoperator_t* result = new cabindb_mergeoperator_t;
  result->state_ = state;
  result->destructor_ = destructor;
  result->full_merge_ = full_merge;
  result->partial_merge_ = partial_merge;
  result->delete_value_ = delete_value;
  result->name_ = name;
  return result;
}

void cabindb_mergeoperator_destroy(cabindb_mergeoperator_t* merge_operator) {
  delete merge_operator;
}

cabindb_readoptions_t* cabindb_readoptions_create() {
  return new cabindb_readoptions_t;
}

void cabindb_readoptions_destroy(cabindb_readoptions_t* opt) {
  delete opt;
}

void cabindb_readoptions_set_verify_checksums(
    cabindb_readoptions_t* opt,
    unsigned char v) {
  opt->rep.verify_checksums = v;
}

unsigned char cabindb_readoptions_get_verify_checksums(
    cabindb_readoptions_t* opt) {
  return opt->rep.verify_checksums;
}

void cabindb_readoptions_set_fill_cache(
    cabindb_readoptions_t* opt, unsigned char v) {
  opt->rep.fill_cache = v;
}

unsigned char cabindb_readoptions_get_fill_cache(cabindb_readoptions_t* opt) {
  return opt->rep.fill_cache;
}

void cabindb_readoptions_set_snapshot(
    cabindb_readoptions_t* opt,
    const cabindb_snapshot_t* snap) {
  opt->rep.snapshot = (snap ? snap->rep : nullptr);
}

void cabindb_readoptions_set_iterate_upper_bound(
    cabindb_readoptions_t* opt,
    const char* key, size_t keylen) {
  if (key == nullptr) {
    opt->upper_bound = Slice();
    opt->rep.iterate_upper_bound = nullptr;

  } else {
    opt->upper_bound = Slice(key, keylen);
    opt->rep.iterate_upper_bound = &opt->upper_bound;
  }
}

void cabindb_readoptions_set_iterate_lower_bound(
    cabindb_readoptions_t *opt,
    const char* key, size_t keylen) {
  if (key == nullptr) {
    opt->lower_bound = Slice();
    opt->rep.iterate_lower_bound = nullptr;
  } else {
    opt->lower_bound = Slice(key, keylen);
    opt->rep.iterate_lower_bound = &opt->lower_bound;
  }
}

void cabindb_readoptions_set_read_tier(
    cabindb_readoptions_t* opt, int v) {
  opt->rep.read_tier = static_cast<CABINDB_NAMESPACE::ReadTier>(v);
}

int cabindb_readoptions_get_read_tier(cabindb_readoptions_t* opt) {
  return static_cast<int>(opt->rep.read_tier);
}

void cabindb_readoptions_set_tailing(
    cabindb_readoptions_t* opt, unsigned char v) {
  opt->rep.tailing = v;
}

unsigned char cabindb_readoptions_get_tailing(cabindb_readoptions_t* opt) {
  return opt->rep.tailing;
}

void cabindb_readoptions_set_managed(
    cabindb_readoptions_t* opt, unsigned char v) {
  opt->rep.managed = v;
}

void cabindb_readoptions_set_readahead_size(
    cabindb_readoptions_t* opt, size_t v) {
  opt->rep.readahead_size = v;
}

size_t cabindb_readoptions_get_readahead_size(cabindb_readoptions_t* opt) {
  return opt->rep.readahead_size;
}

void cabindb_readoptions_set_prefix_same_as_start(
    cabindb_readoptions_t* opt, unsigned char v) {
  opt->rep.prefix_same_as_start = v;
}

unsigned char cabindb_readoptions_get_prefix_same_as_start(
    cabindb_readoptions_t* opt) {
  return opt->rep.prefix_same_as_start;
}

void cabindb_readoptions_set_pin_data(cabindb_readoptions_t* opt,
                                      unsigned char v) {
  opt->rep.pin_data = v;
}

unsigned char cabindb_readoptions_get_pin_data(cabindb_readoptions_t* opt) {
  return opt->rep.pin_data;
}

void cabindb_readoptions_set_total_order_seek(cabindb_readoptions_t* opt,
                                              unsigned char v) {
  opt->rep.total_order_seek = v;
}

unsigned char cabindb_readoptions_get_total_order_seek(
    cabindb_readoptions_t* opt) {
  return opt->rep.total_order_seek;
}

void cabindb_readoptions_set_max_skippable_internal_keys(
    cabindb_readoptions_t* opt,
    uint64_t v) {
  opt->rep.max_skippable_internal_keys = v;
}

uint64_t cabindb_readoptions_get_max_skippable_internal_keys(
    cabindb_readoptions_t* opt) {
  return opt->rep.max_skippable_internal_keys;
}

void cabindb_readoptions_set_background_purge_on_iterator_cleanup(
    cabindb_readoptions_t* opt, unsigned char v) {
  opt->rep.background_purge_on_iterator_cleanup = v;
}

unsigned char cabindb_readoptions_get_background_purge_on_iterator_cleanup(
    cabindb_readoptions_t* opt) {
  return opt->rep.background_purge_on_iterator_cleanup;
}

void cabindb_readoptions_set_ignore_range_deletions(
    cabindb_readoptions_t* opt, unsigned char v) {
  opt->rep.ignore_range_deletions = v;
}

unsigned char cabindb_readoptions_get_ignore_range_deletions(
    cabindb_readoptions_t* opt) {
  return opt->rep.ignore_range_deletions;
}

cabindb_writeoptions_t* cabindb_writeoptions_create() {
  return new cabindb_writeoptions_t;
}

void cabindb_writeoptions_destroy(cabindb_writeoptions_t* opt) {
  delete opt;
}

void cabindb_writeoptions_set_sync(
    cabindb_writeoptions_t* opt, unsigned char v) {
  opt->rep.sync = v;
}

unsigned char cabindb_writeoptions_get_sync(cabindb_writeoptions_t* opt) {
  return opt->rep.sync;
}

void cabindb_writeoptions_disable_WAL(cabindb_writeoptions_t* opt, int disable) {
  opt->rep.disableWAL = disable;
}

unsigned char cabindb_writeoptions_get_disable_WAL(
    cabindb_writeoptions_t* opt) {
  return opt->rep.disableWAL;
}

void cabindb_writeoptions_set_ignore_missing_column_families(
    cabindb_writeoptions_t* opt,
    unsigned char v) {
  opt->rep.ignore_missing_column_families = v;
}

unsigned char cabindb_writeoptions_get_ignore_missing_column_families(
    cabindb_writeoptions_t* opt) {
  return opt->rep.ignore_missing_column_families;
}

void cabindb_writeoptions_set_no_slowdown(
    cabindb_writeoptions_t* opt,
    unsigned char v) {
  opt->rep.no_slowdown = v;
}

unsigned char cabindb_writeoptions_get_no_slowdown(
    cabindb_writeoptions_t* opt) {
  return opt->rep.no_slowdown;
}

void cabindb_writeoptions_set_low_pri(
    cabindb_writeoptions_t* opt,
    unsigned char v) {
  opt->rep.low_pri = v;
}

unsigned char cabindb_writeoptions_get_low_pri(cabindb_writeoptions_t* opt) {
  return opt->rep.low_pri;
}

void cabindb_writeoptions_set_memtable_insert_hint_per_batch(
    cabindb_writeoptions_t* opt, unsigned char v) {
  opt->rep.memtable_insert_hint_per_batch = v;
}

unsigned char cabindb_writeoptions_get_memtable_insert_hint_per_batch(
    cabindb_writeoptions_t* opt) {
  return opt->rep.memtable_insert_hint_per_batch;
}

cabindb_compactoptions_t* cabindb_compactoptions_create() {
  return new cabindb_compactoptions_t;
}

void cabindb_compactoptions_destroy(cabindb_compactoptions_t* opt) {
  delete opt;
}

void cabindb_compactoptions_set_bottommost_level_compaction(
    cabindb_compactoptions_t* opt, unsigned char v) {
  opt->rep.bottommost_level_compaction = static_cast<BottommostLevelCompaction>(v);
}

unsigned char cabindb_compactoptions_get_bottommost_level_compaction(
    cabindb_compactoptions_t* opt) {
  return static_cast<unsigned char>(opt->rep.bottommost_level_compaction);
}

void cabindb_compactoptions_set_exclusive_manual_compaction(
    cabindb_compactoptions_t* opt, unsigned char v) {
  opt->rep.exclusive_manual_compaction = v;
}

unsigned char cabindb_compactoptions_get_exclusive_manual_compaction(
    cabindb_compactoptions_t* opt) {
  return opt->rep.exclusive_manual_compaction;
}

void cabindb_compactoptions_set_change_level(cabindb_compactoptions_t* opt,
                                             unsigned char v) {
  opt->rep.change_level = v;
}

unsigned char cabindb_compactoptions_get_change_level(
    cabindb_compactoptions_t* opt) {
  return opt->rep.change_level;
}

void cabindb_compactoptions_set_target_level(cabindb_compactoptions_t* opt,
                                             int n) {
  opt->rep.target_level = n;
}

int cabindb_compactoptions_get_target_level(cabindb_compactoptions_t* opt) {
  return opt->rep.target_level;
}

cabindb_flushoptions_t* cabindb_flushoptions_create() {
  return new cabindb_flushoptions_t;
}

void cabindb_flushoptions_destroy(cabindb_flushoptions_t* opt) {
  delete opt;
}

void cabindb_flushoptions_set_wait(
    cabindb_flushoptions_t* opt, unsigned char v) {
  opt->rep.wait = v;
}

unsigned char cabindb_flushoptions_get_wait(cabindb_flushoptions_t* opt) {
  return opt->rep.wait;
}

cabindb_cache_t* cabindb_cache_create_lru(size_t capacity) {
  cabindb_cache_t* c = new cabindb_cache_t;
  c->rep = NewLRUCache(capacity);
  return c;
}

void cabindb_cache_destroy(cabindb_cache_t* cache) {
  delete cache;
}

void cabindb_cache_set_capacity(cabindb_cache_t* cache, size_t capacity) {
  cache->rep->SetCapacity(capacity);
}

size_t cabindb_cache_get_capacity(cabindb_cache_t* cache) {
  return cache->rep->GetCapacity();
}

size_t cabindb_cache_get_usage(cabindb_cache_t* cache) {
  return cache->rep->GetUsage();
}

size_t cabindb_cache_get_pinned_usage(cabindb_cache_t* cache) {
  return cache->rep->GetPinnedUsage();
}

cabindb_dbpath_t* cabindb_dbpath_create(const char* path, uint64_t target_size) {
  cabindb_dbpath_t* result = new cabindb_dbpath_t;
  result->rep.path = std::string(path);
  result->rep.target_size = target_size;
  return result;
}

void cabindb_dbpath_destroy(cabindb_dbpath_t* dbpath) {
  delete dbpath;
}

cabindb_env_t* cabindb_create_default_env() {
  cabindb_env_t* result = new cabindb_env_t;
  result->rep = Env::Default();
  result->is_default = true;
  return result;
}

cabindb_env_t* cabindb_create_mem_env() {
  cabindb_env_t* result = new cabindb_env_t;
  result->rep = CABINDB_NAMESPACE::NewMemEnv(Env::Default());
  result->is_default = false;
  return result;
}

void cabindb_env_set_background_threads(cabindb_env_t* env, int n) {
  env->rep->SetBackgroundThreads(n);
}

int cabindb_env_get_background_threads(cabindb_env_t* env) {
  return env->rep->GetBackgroundThreads();
}

void cabindb_env_set_bottom_priority_background_threads(cabindb_env_t* env,
                                                        int n) {
  env->rep->SetBackgroundThreads(n, Env::BOTTOM);
}

int cabindb_env_get_bottom_priority_background_threads(cabindb_env_t* env) {
  return env->rep->GetBackgroundThreads(Env::BOTTOM);
}

void cabindb_env_set_high_priority_background_threads(cabindb_env_t* env, int n) {
  env->rep->SetBackgroundThreads(n, Env::HIGH);
}

int cabindb_env_get_high_priority_background_threads(cabindb_env_t* env) {
  return env->rep->GetBackgroundThreads(Env::HIGH);
}

void cabindb_env_set_low_priority_background_threads(cabindb_env_t* env,
                                                     int n) {
  env->rep->SetBackgroundThreads(n, Env::LOW);
}

int cabindb_env_get_low_priority_background_threads(cabindb_env_t* env) {
  return env->rep->GetBackgroundThreads(Env::LOW);
}

void cabindb_env_join_all_threads(cabindb_env_t* env) {
  env->rep->WaitForJoin();
}

void cabindb_env_lower_thread_pool_io_priority(cabindb_env_t* env) {
  env->rep->LowerThreadPoolIOPriority();
}

void cabindb_env_lower_high_priority_thread_pool_io_priority(cabindb_env_t* env) {
  env->rep->LowerThreadPoolIOPriority(Env::HIGH);
}

void cabindb_env_lower_thread_pool_cpu_priority(cabindb_env_t* env) {
  env->rep->LowerThreadPoolCPUPriority();
}

void cabindb_env_lower_high_priority_thread_pool_cpu_priority(cabindb_env_t* env) {
  env->rep->LowerThreadPoolCPUPriority(Env::HIGH);
}

void cabindb_env_destroy(cabindb_env_t* env) {
  if (!env->is_default) delete env->rep;
  delete env;
}

cabindb_envoptions_t* cabindb_envoptions_create() {
  cabindb_envoptions_t* opt = new cabindb_envoptions_t;
  return opt;
}

void cabindb_envoptions_destroy(cabindb_envoptions_t* opt) { delete opt; }

cabindb_sstfilewriter_t* cabindb_sstfilewriter_create(
    const cabindb_envoptions_t* env, const cabindb_options_t* io_options) {
  cabindb_sstfilewriter_t* writer = new cabindb_sstfilewriter_t;
  writer->rep = new SstFileWriter(env->rep, io_options->rep);
  return writer;
}

cabindb_sstfilewriter_t* cabindb_sstfilewriter_create_with_comparator(
    const cabindb_envoptions_t* env, const cabindb_options_t* io_options,
    const cabindb_comparator_t* /*comparator*/) {
  cabindb_sstfilewriter_t* writer = new cabindb_sstfilewriter_t;
  writer->rep = new SstFileWriter(env->rep, io_options->rep);
  return writer;
}

void cabindb_sstfilewriter_open(cabindb_sstfilewriter_t* writer,
                                const char* name, char** errptr) {
  SaveError(errptr, writer->rep->Open(std::string(name)));
}

void cabindb_sstfilewriter_add(cabindb_sstfilewriter_t* writer, const char* key,
                               size_t keylen, const char* val, size_t vallen,
                               char** errptr) {
  SaveError(errptr, writer->rep->Put(Slice(key, keylen), Slice(val, vallen)));
}

void cabindb_sstfilewriter_put(cabindb_sstfilewriter_t* writer, const char* key,
                               size_t keylen, const char* val, size_t vallen,
                               char** errptr) {
  SaveError(errptr, writer->rep->Put(Slice(key, keylen), Slice(val, vallen)));
}

void cabindb_sstfilewriter_merge(cabindb_sstfilewriter_t* writer,
                                 const char* key, size_t keylen,
                                 const char* val, size_t vallen,
                                 char** errptr) {
  SaveError(errptr, writer->rep->Merge(Slice(key, keylen), Slice(val, vallen)));
}

void cabindb_sstfilewriter_delete(cabindb_sstfilewriter_t* writer,
                                  const char* key, size_t keylen,
                                  char** errptr) {
  SaveError(errptr, writer->rep->Delete(Slice(key, keylen)));
}

void cabindb_sstfilewriter_finish(cabindb_sstfilewriter_t* writer,
                                  char** errptr) {
  SaveError(errptr, writer->rep->Finish(nullptr));
}

void cabindb_sstfilewriter_file_size(cabindb_sstfilewriter_t* writer,
                                  uint64_t* file_size) {
  *file_size = writer->rep->FileSize();
}

void cabindb_sstfilewriter_destroy(cabindb_sstfilewriter_t* writer) {
  delete writer->rep;
  delete writer;
}

cabindb_ingestexternalfileoptions_t*
cabindb_ingestexternalfileoptions_create() {
  cabindb_ingestexternalfileoptions_t* opt =
      new cabindb_ingestexternalfileoptions_t;
  return opt;
}

void cabindb_ingestexternalfileoptions_set_move_files(
    cabindb_ingestexternalfileoptions_t* opt, unsigned char move_files) {
  opt->rep.move_files = move_files;
}

void cabindb_ingestexternalfileoptions_set_snapshot_consistency(
    cabindb_ingestexternalfileoptions_t* opt,
    unsigned char snapshot_consistency) {
  opt->rep.snapshot_consistency = snapshot_consistency;
}

void cabindb_ingestexternalfileoptions_set_allow_global_seqno(
    cabindb_ingestexternalfileoptions_t* opt,
    unsigned char allow_global_seqno) {
  opt->rep.allow_global_seqno = allow_global_seqno;
}

void cabindb_ingestexternalfileoptions_set_allow_blocking_flush(
    cabindb_ingestexternalfileoptions_t* opt,
    unsigned char allow_blocking_flush) {
  opt->rep.allow_blocking_flush = allow_blocking_flush;
}

void cabindb_ingestexternalfileoptions_set_ingest_behind(
    cabindb_ingestexternalfileoptions_t* opt,
    unsigned char ingest_behind) {
  opt->rep.ingest_behind = ingest_behind;
}

void cabindb_ingestexternalfileoptions_destroy(
    cabindb_ingestexternalfileoptions_t* opt) {
  delete opt;
}

void cabindb_ingest_external_file(
    cabindb_t* db, const char* const* file_list, const size_t list_len,
    const cabindb_ingestexternalfileoptions_t* opt, char** errptr) {
  std::vector<std::string> files(list_len);
  for (size_t i = 0; i < list_len; ++i) {
    files[i] = std::string(file_list[i]);
  }
  SaveError(errptr, db->rep->IngestExternalFile(files, opt->rep));
}

void cabindb_ingest_external_file_cf(
    cabindb_t* db, cabindb_column_family_handle_t* handle,
    const char* const* file_list, const size_t list_len,
    const cabindb_ingestexternalfileoptions_t* opt, char** errptr) {
  std::vector<std::string> files(list_len);
  for (size_t i = 0; i < list_len; ++i) {
    files[i] = std::string(file_list[i]);
  }
  SaveError(errptr, db->rep->IngestExternalFile(handle->rep, files, opt->rep));
}

void cabindb_try_catch_up_with_primary(cabindb_t* db, char** errptr) {
  SaveError(errptr, db->rep->TryCatchUpWithPrimary());
}

cabindb_slicetransform_t* cabindb_slicetransform_create(
    void* state,
    void (*destructor)(void*),
    char* (*transform)(
        void*,
        const char* key, size_t length,
        size_t* dst_length),
    unsigned char (*in_domain)(
        void*,
        const char* key, size_t length),
    unsigned char (*in_range)(
        void*,
        const char* key, size_t length),
    const char* (*name)(void*)) {
  cabindb_slicetransform_t* result = new cabindb_slicetransform_t;
  result->state_ = state;
  result->destructor_ = destructor;
  result->transform_ = transform;
  result->in_domain_ = in_domain;
  result->in_range_ = in_range;
  result->name_ = name;
  return result;
}

void cabindb_slicetransform_destroy(cabindb_slicetransform_t* st) {
  delete st;
}

struct Wrapper : public cabindb_slicetransform_t {
  const SliceTransform* rep_;
  ~Wrapper() override { delete rep_; }
  const char* Name() const override { return rep_->Name(); }
  Slice Transform(const Slice& src) const override {
    return rep_->Transform(src);
  }
  bool InDomain(const Slice& src) const override {
    return rep_->InDomain(src);
  }
  bool InRange(const Slice& src) const override { return rep_->InRange(src); }
  static void DoNothing(void*) { }
};

cabindb_slicetransform_t* cabindb_slicetransform_create_fixed_prefix(size_t prefixLen) {
  Wrapper* wrapper = new Wrapper;
  wrapper->rep_ = CABINDB_NAMESPACE::NewFixedPrefixTransform(prefixLen);
  wrapper->state_ = nullptr;
  wrapper->destructor_ = &Wrapper::DoNothing;
  return wrapper;
}

cabindb_slicetransform_t* cabindb_slicetransform_create_noop() {
  Wrapper* wrapper = new Wrapper;
  wrapper->rep_ = CABINDB_NAMESPACE::NewNoopTransform();
  wrapper->state_ = nullptr;
  wrapper->destructor_ = &Wrapper::DoNothing;
  return wrapper;
}

cabindb_universal_compaction_options_t* cabindb_universal_compaction_options_create() {
  cabindb_universal_compaction_options_t* result = new cabindb_universal_compaction_options_t;
  result->rep = new CABINDB_NAMESPACE::CompactionOptionsUniversal;
  return result;
}

void cabindb_universal_compaction_options_set_size_ratio(
  cabindb_universal_compaction_options_t* uco, int ratio) {
  uco->rep->size_ratio = ratio;
}

int cabindb_universal_compaction_options_get_size_ratio(
    cabindb_universal_compaction_options_t* uco) {
  return uco->rep->size_ratio;
}

void cabindb_universal_compaction_options_set_min_merge_width(
  cabindb_universal_compaction_options_t* uco, int w) {
  uco->rep->min_merge_width = w;
}

int cabindb_universal_compaction_options_get_min_merge_width(
    cabindb_universal_compaction_options_t* uco) {
  return uco->rep->min_merge_width;
}

void cabindb_universal_compaction_options_set_max_merge_width(
  cabindb_universal_compaction_options_t* uco, int w) {
  uco->rep->max_merge_width = w;
}

int cabindb_universal_compaction_options_get_max_merge_width(
    cabindb_universal_compaction_options_t* uco) {
  return uco->rep->max_merge_width;
}

void cabindb_universal_compaction_options_set_max_size_amplification_percent(
  cabindb_universal_compaction_options_t* uco, int p) {
  uco->rep->max_size_amplification_percent = p;
}

int cabindb_universal_compaction_options_get_max_size_amplification_percent(
    cabindb_universal_compaction_options_t* uco) {
  return uco->rep->max_size_amplification_percent;
}

void cabindb_universal_compaction_options_set_compression_size_percent(
  cabindb_universal_compaction_options_t* uco, int p) {
  uco->rep->compression_size_percent = p;
}

int cabindb_universal_compaction_options_get_compression_size_percent(
    cabindb_universal_compaction_options_t* uco) {
  return uco->rep->compression_size_percent;
}

void cabindb_universal_compaction_options_set_stop_style(
  cabindb_universal_compaction_options_t* uco, int style) {
  uco->rep->stop_style =
      static_cast<CABINDB_NAMESPACE::CompactionStopStyle>(style);
}

int cabindb_universal_compaction_options_get_stop_style(
    cabindb_universal_compaction_options_t* uco) {
  return static_cast<int>(uco->rep->stop_style);
}

void cabindb_universal_compaction_options_destroy(
  cabindb_universal_compaction_options_t* uco) {
  delete uco->rep;
  delete uco;
}

cabindb_fifo_compaction_options_t* cabindb_fifo_compaction_options_create() {
  cabindb_fifo_compaction_options_t* result = new cabindb_fifo_compaction_options_t;
  result->rep =  CompactionOptionsFIFO();
  return result;
}

void cabindb_fifo_compaction_options_set_max_table_files_size(
    cabindb_fifo_compaction_options_t* fifo_opts, uint64_t size) {
  fifo_opts->rep.max_table_files_size = size;
}

uint64_t cabindb_fifo_compaction_options_get_max_table_files_size(
    cabindb_fifo_compaction_options_t* fifo_opts) {
  return fifo_opts->rep.max_table_files_size;
}

void cabindb_fifo_compaction_options_destroy(
    cabindb_fifo_compaction_options_t* fifo_opts) {
  delete fifo_opts;
}

void cabindb_options_set_min_level_to_compress(cabindb_options_t* opt, int level) {
  if (level >= 0) {
    assert(level <= opt->rep.num_levels);
    opt->rep.compression_per_level.resize(opt->rep.num_levels);
    for (int i = 0; i < level; i++) {
      opt->rep.compression_per_level[i] = CABINDB_NAMESPACE::kNoCompression;
    }
    for (int i = level; i < opt->rep.num_levels; i++) {
      opt->rep.compression_per_level[i] = opt->rep.compression;
    }
  }
}

int cabindb_livefiles_count(
  const cabindb_livefiles_t* lf) {
  return static_cast<int>(lf->rep.size());
}

const char* cabindb_livefiles_name(
  const cabindb_livefiles_t* lf,
  int index) {
  return lf->rep[index].name.c_str();
}

int cabindb_livefiles_level(
  const cabindb_livefiles_t* lf,
  int index) {
  return lf->rep[index].level;
}

size_t cabindb_livefiles_size(
  const cabindb_livefiles_t* lf,
  int index) {
  return lf->rep[index].size;
}

const char* cabindb_livefiles_smallestkey(
  const cabindb_livefiles_t* lf,
  int index,
  size_t* size) {
  *size = lf->rep[index].smallestkey.size();
  return lf->rep[index].smallestkey.data();
}

const char* cabindb_livefiles_largestkey(
  const cabindb_livefiles_t* lf,
  int index,
  size_t* size) {
  *size = lf->rep[index].largestkey.size();
  return lf->rep[index].largestkey.data();
}

uint64_t cabindb_livefiles_entries(
    const cabindb_livefiles_t* lf,
    int index) {
  return lf->rep[index].num_entries;
}

uint64_t cabindb_livefiles_deletions(
    const cabindb_livefiles_t* lf,
    int index) {
  return lf->rep[index].num_deletions;
}

extern void cabindb_livefiles_destroy(
  const cabindb_livefiles_t* lf) {
  delete lf;
}

void cabindb_get_options_from_string(const cabindb_options_t* base_options,
                                     const char* opts_str,
                                     cabindb_options_t* new_options,
                                     char** errptr) {
  SaveError(errptr,
            GetOptionsFromString(base_options->rep, std::string(opts_str),
                                 &new_options->rep));
}

void cabindb_delete_file_in_range(cabindb_t* db, const char* start_key,
                                  size_t start_key_len, const char* limit_key,
                                  size_t limit_key_len, char** errptr) {
  Slice a, b;
  SaveError(
      errptr,
      DeleteFilesInRange(
          db->rep, db->rep->DefaultColumnFamily(),
          (start_key ? (a = Slice(start_key, start_key_len), &a) : nullptr),
          (limit_key ? (b = Slice(limit_key, limit_key_len), &b) : nullptr)));
}

void cabindb_delete_file_in_range_cf(
    cabindb_t* db, cabindb_column_family_handle_t* column_family,
    const char* start_key, size_t start_key_len, const char* limit_key,
    size_t limit_key_len, char** errptr) {
  Slice a, b;
  SaveError(
      errptr,
      DeleteFilesInRange(
          db->rep, column_family->rep,
          (start_key ? (a = Slice(start_key, start_key_len), &a) : nullptr),
          (limit_key ? (b = Slice(limit_key, limit_key_len), &b) : nullptr)));
}

cabindb_transactiondb_options_t* cabindb_transactiondb_options_create() {
  return new cabindb_transactiondb_options_t;
}

void cabindb_transactiondb_options_destroy(cabindb_transactiondb_options_t* opt){
  delete opt;
}

void cabindb_transactiondb_options_set_max_num_locks(
    cabindb_transactiondb_options_t* opt, int64_t max_num_locks) {
  opt->rep.max_num_locks = max_num_locks;
}

void cabindb_transactiondb_options_set_num_stripes(
    cabindb_transactiondb_options_t* opt, size_t num_stripes) {
  opt->rep.num_stripes = num_stripes;
}

void cabindb_transactiondb_options_set_transaction_lock_timeout(
    cabindb_transactiondb_options_t* opt, int64_t txn_lock_timeout) {
  opt->rep.transaction_lock_timeout = txn_lock_timeout;
}

void cabindb_transactiondb_options_set_default_lock_timeout(
    cabindb_transactiondb_options_t* opt, int64_t default_lock_timeout) {
  opt->rep.default_lock_timeout = default_lock_timeout;
}

cabindb_transaction_options_t* cabindb_transaction_options_create() {
  return new cabindb_transaction_options_t;
}

void cabindb_transaction_options_destroy(cabindb_transaction_options_t* opt) {
  delete opt;
}

void cabindb_transaction_options_set_set_snapshot(
    cabindb_transaction_options_t* opt, unsigned char v) {
  opt->rep.set_snapshot = v;
}

void cabindb_transaction_options_set_deadlock_detect(
    cabindb_transaction_options_t* opt, unsigned char v) {
  opt->rep.deadlock_detect = v;
}

void cabindb_transaction_options_set_lock_timeout(
    cabindb_transaction_options_t* opt, int64_t lock_timeout) {
  opt->rep.lock_timeout = lock_timeout;
}

void cabindb_transaction_options_set_expiration(
    cabindb_transaction_options_t* opt, int64_t expiration) {
  opt->rep.expiration = expiration;
}

void cabindb_transaction_options_set_deadlock_detect_depth(
    cabindb_transaction_options_t* opt, int64_t depth) {
  opt->rep.deadlock_detect_depth = depth;
}

void cabindb_transaction_options_set_max_write_batch_size(
    cabindb_transaction_options_t* opt, size_t size) {
  opt->rep.max_write_batch_size = size;
}

cabindb_optimistictransaction_options_t*
cabindb_optimistictransaction_options_create() {
  return new cabindb_optimistictransaction_options_t;
}

void cabindb_optimistictransaction_options_destroy(
    cabindb_optimistictransaction_options_t* opt) {
  delete opt;
}

void cabindb_optimistictransaction_options_set_set_snapshot(
    cabindb_optimistictransaction_options_t* opt, unsigned char v) {
  opt->rep.set_snapshot = v;
}

cabindb_column_family_handle_t* cabindb_transactiondb_create_column_family(
    cabindb_transactiondb_t* txn_db,
    const cabindb_options_t* column_family_options,
    const char* column_family_name, char** errptr) {
  cabindb_column_family_handle_t* handle = new cabindb_column_family_handle_t;
  SaveError(errptr, txn_db->rep->CreateColumnFamily(
                        ColumnFamilyOptions(column_family_options->rep),
                        std::string(column_family_name), &(handle->rep)));
  return handle;
}

cabindb_transactiondb_t* cabindb_transactiondb_open(
    const cabindb_options_t* options,
    const cabindb_transactiondb_options_t* txn_db_options, const char* name,
    char** errptr) {
  TransactionDB* txn_db;
  if (SaveError(errptr, TransactionDB::Open(options->rep, txn_db_options->rep,
                                            std::string(name), &txn_db))) {
    return nullptr;
  }
  cabindb_transactiondb_t* result = new cabindb_transactiondb_t;
  result->rep = txn_db;
  return result;
}

cabindb_transactiondb_t* cabindb_transactiondb_open_column_families(
    const cabindb_options_t* options,
    const cabindb_transactiondb_options_t* txn_db_options, const char* name,
    int num_column_families, const char* const* column_family_names,
    const cabindb_options_t* const* column_family_options,
    cabindb_column_family_handle_t** column_family_handles, char** errptr) {
  std::vector<ColumnFamilyDescriptor> column_families;
  for (int i = 0; i < num_column_families; i++) {
    column_families.push_back(ColumnFamilyDescriptor(
        std::string(column_family_names[i]),
        ColumnFamilyOptions(column_family_options[i]->rep)));
  }

  TransactionDB* txn_db;
  std::vector<ColumnFamilyHandle*> handles;
  if (SaveError(errptr, TransactionDB::Open(options->rep, txn_db_options->rep,
                                            std::string(name), column_families,
                                            &handles, &txn_db))) {
    return nullptr;
  }

  for (size_t i = 0; i < handles.size(); i++) {
    cabindb_column_family_handle_t* c_handle =
        new cabindb_column_family_handle_t;
    c_handle->rep = handles[i];
    column_family_handles[i] = c_handle;
  }
  cabindb_transactiondb_t* result = new cabindb_transactiondb_t;
  result->rep = txn_db;
  return result;
}

const cabindb_snapshot_t* cabindb_transactiondb_create_snapshot(
    cabindb_transactiondb_t* txn_db) {
  cabindb_snapshot_t* result = new cabindb_snapshot_t;
  result->rep = txn_db->rep->GetSnapshot();
  return result;
}

void cabindb_transactiondb_release_snapshot(
    cabindb_transactiondb_t* txn_db, const cabindb_snapshot_t* snapshot) {
  txn_db->rep->ReleaseSnapshot(snapshot->rep);
  delete snapshot;
}

cabindb_transaction_t* cabindb_transaction_begin(
    cabindb_transactiondb_t* txn_db,
    const cabindb_writeoptions_t* write_options,
    const cabindb_transaction_options_t* txn_options,
    cabindb_transaction_t* old_txn) {
  if (old_txn == nullptr) {
    cabindb_transaction_t* result = new cabindb_transaction_t;
    result->rep = txn_db->rep->BeginTransaction(write_options->rep,
                                                txn_options->rep, nullptr);
    return result;
  }
  old_txn->rep = txn_db->rep->BeginTransaction(write_options->rep,
                                                txn_options->rep, old_txn->rep);
  return old_txn;
}

void cabindb_transaction_commit(cabindb_transaction_t* txn, char** errptr) {
  SaveError(errptr, txn->rep->Commit());
}

void cabindb_transaction_rollback(cabindb_transaction_t* txn, char** errptr) {
  SaveError(errptr, txn->rep->Rollback());
}

void cabindb_transaction_set_savepoint(cabindb_transaction_t* txn) {
  txn->rep->SetSavePoint();
}

void cabindb_transaction_rollback_to_savepoint(cabindb_transaction_t* txn, char** errptr) {
  SaveError(errptr, txn->rep->RollbackToSavePoint());
}

void cabindb_transaction_destroy(cabindb_transaction_t* txn) {
  delete txn->rep;
  delete txn;
}

const cabindb_snapshot_t* cabindb_transaction_get_snapshot(
    cabindb_transaction_t* txn) {
  cabindb_snapshot_t* result = new cabindb_snapshot_t;
  result->rep = txn->rep->GetSnapshot();
  return result;
}

// Read a key inside a transaction
char* cabindb_transaction_get(cabindb_transaction_t* txn,
                              const cabindb_readoptions_t* options,
                              const char* key, size_t klen, size_t* vlen,
                              char** errptr) {
  char* result = nullptr;
  std::string tmp;
  Status s = txn->rep->Get(options->rep, Slice(key, klen), &tmp);
  if (s.ok()) {
    *vlen = tmp.size();
    result = CopyString(tmp);
  } else {
    *vlen = 0;
    if (!s.IsNotFound()) {
      SaveError(errptr, s);
    }
  }
  return result;
}

char* cabindb_transaction_get_cf(cabindb_transaction_t* txn,
                                 const cabindb_readoptions_t* options,
                                 cabindb_column_family_handle_t* column_family,
                                 const char* key, size_t klen, size_t* vlen,
                                 char** errptr) {
  char* result = nullptr;
  std::string tmp;
  Status s =
      txn->rep->Get(options->rep, column_family->rep, Slice(key, klen), &tmp);
  if (s.ok()) {
    *vlen = tmp.size();
    result = CopyString(tmp);
  } else {
    *vlen = 0;
    if (!s.IsNotFound()) {
      SaveError(errptr, s);
    }
  }
  return result;
}

// Read a key inside a transaction
char* cabindb_transaction_get_for_update(cabindb_transaction_t* txn,
                                         const cabindb_readoptions_t* options,
                                         const char* key, size_t klen,
                                         size_t* vlen, unsigned char exclusive,
                                         char** errptr) {
  char* result = nullptr;
  std::string tmp;
  Status s =
      txn->rep->GetForUpdate(options->rep, Slice(key, klen), &tmp, exclusive);
  if (s.ok()) {
    *vlen = tmp.size();
    result = CopyString(tmp);
  } else {
    *vlen = 0;
    if (!s.IsNotFound()) {
      SaveError(errptr, s);
    }
  }
  return result;
}

char* cabindb_transaction_get_for_update_cf(
    cabindb_transaction_t* txn, const cabindb_readoptions_t* options,
    cabindb_column_family_handle_t* column_family, const char* key, size_t klen,
    size_t* vlen, unsigned char exclusive, char** errptr) {
  char* result = nullptr;
  std::string tmp;
  Status s = txn->rep->GetForUpdate(options->rep, column_family->rep,
                                    Slice(key, klen), &tmp, exclusive);
  if (s.ok()) {
    *vlen = tmp.size();
    result = CopyString(tmp);
  } else {
    *vlen = 0;
    if (!s.IsNotFound()) {
      SaveError(errptr, s);
    }
  }
  return result;
}

// Read a key outside a transaction
char* cabindb_transactiondb_get(
    cabindb_transactiondb_t* txn_db,
    const cabindb_readoptions_t* options,
    const char* key, size_t klen,
    size_t* vlen,
    char** errptr){
  char* result = nullptr;
  std::string tmp;
  Status s = txn_db->rep->Get(options->rep, Slice(key, klen), &tmp);
  if (s.ok()) {
    *vlen = tmp.size();
    result = CopyString(tmp);
  } else {
    *vlen = 0;
    if (!s.IsNotFound()) {
      SaveError(errptr, s);
    }
  }
  return result;
}

char* cabindb_transactiondb_get_cf(
    cabindb_transactiondb_t* txn_db, const cabindb_readoptions_t* options,
    cabindb_column_family_handle_t* column_family, const char* key,
    size_t keylen, size_t* vallen, char** errptr) {
  char* result = nullptr;
  std::string tmp;
  Status s = txn_db->rep->Get(options->rep, column_family->rep,
                              Slice(key, keylen), &tmp);
  if (s.ok()) {
    *vallen = tmp.size();
    result = CopyString(tmp);
  } else {
    *vallen = 0;
    if (!s.IsNotFound()) {
      SaveError(errptr, s);
    }
  }
  return result;
}

// Put a key inside a transaction
void cabindb_transaction_put(cabindb_transaction_t* txn, const char* key,
                             size_t klen, const char* val, size_t vlen,
                             char** errptr) {
  SaveError(errptr, txn->rep->Put(Slice(key, klen), Slice(val, vlen)));
}

void cabindb_transaction_put_cf(cabindb_transaction_t* txn,
                                cabindb_column_family_handle_t* column_family,
                                const char* key, size_t klen, const char* val,
                                size_t vlen, char** errptr) {
  SaveError(errptr, txn->rep->Put(column_family->rep, Slice(key, klen),
                                  Slice(val, vlen)));
}

// Put a key outside a transaction
void cabindb_transactiondb_put(cabindb_transactiondb_t* txn_db,
                               const cabindb_writeoptions_t* options,
                               const char* key, size_t klen, const char* val,
                               size_t vlen, char** errptr) {
  SaveError(errptr,
            txn_db->rep->Put(options->rep, Slice(key, klen), Slice(val, vlen)));
}

void cabindb_transactiondb_put_cf(cabindb_transactiondb_t* txn_db,
                                  const cabindb_writeoptions_t* options,
                                  cabindb_column_family_handle_t* column_family,
                                  const char* key, size_t keylen,
                                  const char* val, size_t vallen,
                                  char** errptr) {
  SaveError(errptr, txn_db->rep->Put(options->rep, column_family->rep,
                                     Slice(key, keylen), Slice(val, vallen)));
}

// Write batch into transaction db
void cabindb_transactiondb_write(
        cabindb_transactiondb_t* db,
        const cabindb_writeoptions_t* options,
        cabindb_writebatch_t* batch,
        char** errptr) {
  SaveError(errptr, db->rep->Write(options->rep, &batch->rep));
}

// Merge a key inside a transaction
void cabindb_transaction_merge(cabindb_transaction_t* txn, const char* key,
                               size_t klen, const char* val, size_t vlen,
                               char** errptr) {
  SaveError(errptr, txn->rep->Merge(Slice(key, klen), Slice(val, vlen)));
}

void cabindb_transaction_merge_cf(cabindb_transaction_t* txn,
                                  cabindb_column_family_handle_t* column_family,
                                  const char* key, size_t klen, const char* val,
                                  size_t vlen, char** errptr) {
  SaveError(errptr, txn->rep->Merge(column_family->rep, Slice(key, klen),
                                    Slice(val, vlen)));
}

// Merge a key outside a transaction
void cabindb_transactiondb_merge(cabindb_transactiondb_t* txn_db,
                                 const cabindb_writeoptions_t* options,
                                 const char* key, size_t klen, const char* val,
                                 size_t vlen, char** errptr) {
  SaveError(errptr, txn_db->rep->Merge(options->rep, Slice(key, klen),
                                       Slice(val, vlen)));
}

void cabindb_transactiondb_merge_cf(
    cabindb_transactiondb_t* txn_db, const cabindb_writeoptions_t* options,
    cabindb_column_family_handle_t* column_family, const char* key, size_t klen,
    const char* val, size_t vlen, char** errptr) {
  SaveError(errptr, txn_db->rep->Merge(options->rep, column_family->rep,
                                       Slice(key, klen), Slice(val, vlen)));
}

// Delete a key inside a transaction
void cabindb_transaction_delete(cabindb_transaction_t* txn, const char* key,
                                size_t klen, char** errptr) {
  SaveError(errptr, txn->rep->Delete(Slice(key, klen)));
}

void cabindb_transaction_delete_cf(
    cabindb_transaction_t* txn, cabindb_column_family_handle_t* column_family,
    const char* key, size_t klen, char** errptr) {
  SaveError(errptr, txn->rep->Delete(column_family->rep, Slice(key, klen)));
}

// Delete a key outside a transaction
void cabindb_transactiondb_delete(cabindb_transactiondb_t* txn_db,
                                  const cabindb_writeoptions_t* options,
                                  const char* key, size_t klen, char** errptr) {
  SaveError(errptr, txn_db->rep->Delete(options->rep, Slice(key, klen)));
}

void cabindb_transactiondb_delete_cf(
    cabindb_transactiondb_t* txn_db, const cabindb_writeoptions_t* options,
    cabindb_column_family_handle_t* column_family, const char* key,
    size_t keylen, char** errptr) {
  SaveError(errptr, txn_db->rep->Delete(options->rep, column_family->rep,
                                        Slice(key, keylen)));
}

// Create an iterator inside a transaction
cabindb_iterator_t* cabindb_transaction_create_iterator(
    cabindb_transaction_t* txn, const cabindb_readoptions_t* options) {
  cabindb_iterator_t* result = new cabindb_iterator_t;
  result->rep = txn->rep->GetIterator(options->rep);
  return result;
}

// Create an iterator inside a transaction with column family
cabindb_iterator_t* cabindb_transaction_create_iterator_cf(
    cabindb_transaction_t* txn, const cabindb_readoptions_t* options,
    cabindb_column_family_handle_t* column_family) {
  cabindb_iterator_t* result = new cabindb_iterator_t;
  result->rep = txn->rep->GetIterator(options->rep, column_family->rep);
  return result;
}

// Create an iterator outside a transaction
cabindb_iterator_t* cabindb_transactiondb_create_iterator(
    cabindb_transactiondb_t* txn_db, const cabindb_readoptions_t* options) {
  cabindb_iterator_t* result = new cabindb_iterator_t;
  result->rep = txn_db->rep->NewIterator(options->rep);
  return result;
}

cabindb_iterator_t* cabindb_transactiondb_create_iterator_cf(
    cabindb_transactiondb_t* txn_db, const cabindb_readoptions_t* options,
    cabindb_column_family_handle_t* column_family) {
  cabindb_iterator_t* result = new cabindb_iterator_t;
  result->rep = txn_db->rep->NewIterator(options->rep, column_family->rep);
  return result;
}

void cabindb_transactiondb_close(cabindb_transactiondb_t* txn_db) {
  delete txn_db->rep;
  delete txn_db;
}

cabindb_checkpoint_t* cabindb_transactiondb_checkpoint_object_create(
    cabindb_transactiondb_t* txn_db, char** errptr) {
  Checkpoint* checkpoint;
  if (SaveError(errptr, Checkpoint::Create(txn_db->rep, &checkpoint))) {
    return nullptr;
  }
  cabindb_checkpoint_t* result = new cabindb_checkpoint_t;
  result->rep = checkpoint;
  return result;
}

cabindb_optimistictransactiondb_t* cabindb_optimistictransactiondb_open(
    const cabindb_options_t* options, const char* name, char** errptr) {
  OptimisticTransactionDB* otxn_db;
  if (SaveError(errptr, OptimisticTransactionDB::Open(
                            options->rep, std::string(name), &otxn_db))) {
    return nullptr;
  }
  cabindb_optimistictransactiondb_t* result =
      new cabindb_optimistictransactiondb_t;
  result->rep = otxn_db;
  return result;
}

cabindb_optimistictransactiondb_t*
cabindb_optimistictransactiondb_open_column_families(
    const cabindb_options_t* db_options, const char* name,
    int num_column_families, const char* const* column_family_names,
    const cabindb_options_t* const* column_family_options,
    cabindb_column_family_handle_t** column_family_handles, char** errptr) {
  std::vector<ColumnFamilyDescriptor> column_families;
  for (int i = 0; i < num_column_families; i++) {
    column_families.push_back(ColumnFamilyDescriptor(
        std::string(column_family_names[i]),
        ColumnFamilyOptions(column_family_options[i]->rep)));
  }

  OptimisticTransactionDB* otxn_db;
  std::vector<ColumnFamilyHandle*> handles;
  if (SaveError(errptr, OptimisticTransactionDB::Open(
                            DBOptions(db_options->rep), std::string(name),
                            column_families, &handles, &otxn_db))) {
    return nullptr;
  }

  for (size_t i = 0; i < handles.size(); i++) {
    cabindb_column_family_handle_t* c_handle =
        new cabindb_column_family_handle_t;
    c_handle->rep = handles[i];
    column_family_handles[i] = c_handle;
  }
  cabindb_optimistictransactiondb_t* result =
      new cabindb_optimistictransactiondb_t;
  result->rep = otxn_db;
  return result;
}

cabindb_t* cabindb_optimistictransactiondb_get_base_db(
    cabindb_optimistictransactiondb_t* otxn_db) {
  DB* base_db = otxn_db->rep->GetBaseDB();

  if (base_db != nullptr) {
    cabindb_t* result = new cabindb_t;
    result->rep = base_db;
    return result;
  }

  return nullptr;
}

void cabindb_optimistictransactiondb_close_base_db(cabindb_t* base_db) {
  delete base_db;
}

cabindb_transaction_t* cabindb_optimistictransaction_begin(
    cabindb_optimistictransactiondb_t* otxn_db,
    const cabindb_writeoptions_t* write_options,
    const cabindb_optimistictransaction_options_t* otxn_options,
    cabindb_transaction_t* old_txn) {
  if (old_txn == nullptr) {
    cabindb_transaction_t* result = new cabindb_transaction_t;
    result->rep = otxn_db->rep->BeginTransaction(write_options->rep,
                                                 otxn_options->rep, nullptr);
    return result;
  }
  old_txn->rep = otxn_db->rep->BeginTransaction(
      write_options->rep, otxn_options->rep, old_txn->rep);
  return old_txn;
}

void cabindb_optimistictransactiondb_close(
    cabindb_optimistictransactiondb_t* otxn_db) {
  delete otxn_db->rep;
  delete otxn_db;
}

void cabindb_free(void* ptr) { free(ptr); }

cabindb_pinnableslice_t* cabindb_get_pinned(
    cabindb_t* db, const cabindb_readoptions_t* options, const char* key,
    size_t keylen, char** errptr) {
  cabindb_pinnableslice_t* v = new (cabindb_pinnableslice_t);
  Status s = db->rep->Get(options->rep, db->rep->DefaultColumnFamily(),
                          Slice(key, keylen), &v->rep);
  if (!s.ok()) {
    delete (v);
    if (!s.IsNotFound()) {
      SaveError(errptr, s);
    }
    return nullptr;
  }
  return v;
}

cabindb_pinnableslice_t* cabindb_get_pinned_cf(
    cabindb_t* db, const cabindb_readoptions_t* options,
    cabindb_column_family_handle_t* column_family, const char* key,
    size_t keylen, char** errptr) {
  cabindb_pinnableslice_t* v = new (cabindb_pinnableslice_t);
  Status s = db->rep->Get(options->rep, column_family->rep, Slice(key, keylen),
                          &v->rep);
  if (!s.ok()) {
    delete v;
    if (!s.IsNotFound()) {
      SaveError(errptr, s);
    }
    return nullptr;
  }
  return v;
}

void cabindb_pinnableslice_destroy(cabindb_pinnableslice_t* v) { delete v; }

const char* cabindb_pinnableslice_value(const cabindb_pinnableslice_t* v,
                                        size_t* vlen) {
  if (!v) {
    *vlen = 0;
    return nullptr;
  }

  *vlen = v->rep.size();
  return v->rep.data();
}

// container to keep databases and caches in order to use
// CABINDB_NAMESPACE::MemoryUtil
struct cabindb_memory_consumers_t {
  std::vector<cabindb_t*> dbs;
  std::unordered_set<cabindb_cache_t*> caches;
};

// initializes new container of memory consumers
cabindb_memory_consumers_t* cabindb_memory_consumers_create() {
  return new cabindb_memory_consumers_t;
}

// adds datatabase to the container of memory consumers
void cabindb_memory_consumers_add_db(cabindb_memory_consumers_t* consumers,
                                     cabindb_t* db) {
  consumers->dbs.push_back(db);
}

// adds cache to the container of memory consumers
void cabindb_memory_consumers_add_cache(cabindb_memory_consumers_t* consumers,
                                        cabindb_cache_t* cache) {
  consumers->caches.insert(cache);
}

// deletes container with memory consumers
void cabindb_memory_consumers_destroy(cabindb_memory_consumers_t* consumers) {
  delete consumers;
}

// contains memory usage statistics provided by CABINDB_NAMESPACE::MemoryUtil
struct cabindb_memory_usage_t {
  uint64_t mem_table_total;
  uint64_t mem_table_unflushed;
  uint64_t mem_table_readers_total;
  uint64_t cache_total;
};

// estimates amount of memory occupied by consumers (dbs and caches)
cabindb_memory_usage_t* cabindb_approximate_memory_usage_create(
    cabindb_memory_consumers_t* consumers, char** errptr) {

  vector<DB*> dbs;
  for (auto db : consumers->dbs) {
    dbs.push_back(db->rep);
  }

  unordered_set<const Cache*> cache_set;
  for (auto cache : consumers->caches) {
    cache_set.insert(const_cast<const Cache*>(cache->rep.get()));
  }

  std::map<CABINDB_NAMESPACE::MemoryUtil::UsageType, uint64_t> usage_by_type;

  auto status = MemoryUtil::GetApproximateMemoryUsageByType(dbs, cache_set,
                                                            &usage_by_type);
  if (SaveError(errptr, status)) {
    return nullptr;
  }

  auto result = new cabindb_memory_usage_t;
  result->mem_table_total = usage_by_type[MemoryUtil::kMemTableTotal];
  result->mem_table_unflushed = usage_by_type[MemoryUtil::kMemTableUnFlushed];
  result->mem_table_readers_total = usage_by_type[MemoryUtil::kTableReadersTotal];
  result->cache_total = usage_by_type[MemoryUtil::kCacheTotal];
  return result;
}

uint64_t cabindb_approximate_memory_usage_get_mem_table_total(
    cabindb_memory_usage_t* memory_usage) {
  return memory_usage->mem_table_total;
}

uint64_t cabindb_approximate_memory_usage_get_mem_table_unflushed(
    cabindb_memory_usage_t* memory_usage) {
  return memory_usage->mem_table_unflushed;
}

uint64_t cabindb_approximate_memory_usage_get_mem_table_readers_total(
    cabindb_memory_usage_t* memory_usage) {
  return memory_usage->mem_table_readers_total;
}

uint64_t cabindb_approximate_memory_usage_get_cache_total(
    cabindb_memory_usage_t* memory_usage) {
  return memory_usage->cache_total;
}

void cabindb_options_set_dump_malloc_stats(cabindb_options_t* opt,
                                           unsigned char val) {
  opt->rep.dump_malloc_stats = val;
}

void cabindb_options_set_memtable_whole_key_filtering(cabindb_options_t* opt,
                                                      unsigned char val) {
  opt->rep.memtable_whole_key_filtering = val;
}

// deletes container with memory usage estimates
void cabindb_approximate_memory_usage_destroy(cabindb_memory_usage_t* usage) {
  delete usage;
}

void cabindb_cancel_all_background_work(cabindb_t* db, unsigned char wait) {
  CancelAllBackgroundWork(db->rep, wait);
}

}  // end extern "C"

#endif  // !CABINDB_LITE
