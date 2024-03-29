// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef CABINDB_LITE

#include <stack>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/write_callback.h"
#include "include/cabindb/db.h"
#include "include/cabindb/slice.h"
#include "include/cabindb/snapshot.h"
#include "include/cabindb/status.h"
#include "include/cabindb/types.h"
#include "include/cabindb/utilities/transaction.h"
#include "include/cabindb/utilities/optimistic_transaction_db.h"
#include "include/cabindb/utilities/write_batch_with_index.h"
#include "utilities/transactions/transaction_base.h"
#include "utilities/transactions/transaction_util.h"

namespace CABINDB_NAMESPACE {

class OptimisticTransaction : public TransactionBaseImpl {
 public:
  OptimisticTransaction(OptimisticTransactionDB* db,
                        const WriteOptions& write_options,
                        const OptimisticTransactionOptions& txn_options);
  // No copying allowed
  OptimisticTransaction(const OptimisticTransaction&) = delete;
  void operator=(const OptimisticTransaction&) = delete;

  virtual ~OptimisticTransaction();

  void Reinitialize(OptimisticTransactionDB* txn_db,
                    const WriteOptions& write_options,
                    const OptimisticTransactionOptions& txn_options);

  Status Prepare() override;

  Status Commit() override;

  Status Rollback() override;

  Status SetName(const TransactionName& name) override;

 protected:
  Status TryLock(ColumnFamilyHandle* column_family, const Slice& key,
                 bool read_only, bool exclusive, const bool do_validate = true,
                 const bool assume_tracked = false) override;

 private:
  CABINDB_FIELD_UNUSED OptimisticTransactionDB* const txn_db_;

  friend class OptimisticTransactionCallback;

  void Initialize(const OptimisticTransactionOptions& txn_options);

  // Returns OK if it is safe to commit this transaction.  Returns Status::Busy
  // if there are read or write conflicts that would prevent us from committing
  // OR if we can not determine whether there would be any such conflicts.
  //
  // Should only be called on writer thread.
  Status CheckTransactionForConflicts(DB* db);

  void Clear() override;

  void UnlockGetForUpdate(ColumnFamilyHandle* /* unused */,
                          const Slice& /* unused */) override {
    // Nothing to unlock.
  }

  Status CommitWithSerialValidate();

  Status CommitWithParallelValidate();
};

// Used at commit time to trigger transaction validation
class OptimisticTransactionCallback : public WriteCallback {
 public:
  explicit OptimisticTransactionCallback(OptimisticTransaction* txn)
      : txn_(txn) {}

  Status Callback(DB* db) override {
    return txn_->CheckTransactionForConflicts(db);
  }

  bool AllowWriteBatching() override { return false; }

 private:
  OptimisticTransaction* txn_;
};

}  // namespace CABINDB_NAMESPACE

#endif  // CABINDB_LITE
