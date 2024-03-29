//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#ifndef CABINDB_LITE

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "monitoring/instrumented_mutex.h"
#include "include/cabindb/utilities/transaction.h"
#include "util/autovector.h"
#include "util/hash_map.h"
#include "util/thread_local.h"
#include "utilities/transactions/lock/lock_manager.h"
#include "utilities/transactions/lock/point/point_lock_tracker.h"

namespace CABINDB_NAMESPACE {

class ColumnFamilyHandle;
struct LockInfo;
struct LockMap;
struct LockMapStripe;

struct DeadlockInfoBuffer {
 private:
  std::vector<DeadlockPath> paths_buffer_;
  uint32_t buffer_idx_;
  std::mutex paths_buffer_mutex_;
  std::vector<DeadlockPath> Normalize();

 public:
  explicit DeadlockInfoBuffer(uint32_t n_latest_dlocks)
      : paths_buffer_(n_latest_dlocks), buffer_idx_(0) {}
  void AddNewPath(DeadlockPath path);
  void Resize(uint32_t target_size);
  std::vector<DeadlockPath> PrepareBuffer();
};

struct TrackedTrxInfo {
  autovector<TransactionID> m_neighbors;
  uint32_t m_cf_id;
  bool m_exclusive;
  std::string m_waiting_key;
};

class PointLockManager : public LockManager {
 public:
  PointLockManager(PessimisticTransactionDB* db,
                   const TransactionDBOptions& opt);
  // No copying allowed
  PointLockManager(const PointLockManager&) = delete;
  PointLockManager& operator=(const PointLockManager&) = delete;

  ~PointLockManager() override;

  bool IsPointLockSupported() const override { return true; }

  bool IsRangeLockSupported() const override { return false; }

  const LockTrackerFactory& GetLockTrackerFactory() const override {
    return PointLockTrackerFactory::Get();
  }

  void AddColumnFamily(const ColumnFamilyHandle* cf) override;
  void RemoveColumnFamily(const ColumnFamilyHandle* cf) override;

  Status TryLock(PessimisticTransaction* txn, ColumnFamilyId column_family_id,
                 const std::string& key, Env* env, bool exclusive) override;
  Status TryLock(PessimisticTransaction* txn, ColumnFamilyId column_family_id,
                 const Endpoint& start, const Endpoint& end, Env* env,
                 bool exclusive) override;

  void UnLock(PessimisticTransaction* txn, const LockTracker& tracker,
              Env* env) override;
  void UnLock(PessimisticTransaction* txn, ColumnFamilyId column_family_id,
              const std::string& key, Env* env) override;
  void UnLock(PessimisticTransaction* txn, ColumnFamilyId column_family_id,
              const Endpoint& start, const Endpoint& end, Env* env) override;

  PointLockStatus GetPointLockStatus() override;

  RangeLockStatus GetRangeLockStatus() override;

  std::vector<DeadlockPath> GetDeadlockInfoBuffer() override;

  void Resize(uint32_t new_size) override;

 private:
  PessimisticTransactionDB* txn_db_impl_;

  // Default number of lock map stripes per column family
  const size_t default_num_stripes_;

  // Limit on number of keys locked per column family
  const int64_t max_num_locks_;

  // The following lock order must be satisfied in order to avoid deadlocking
  // ourselves.
  //   - lock_map_mutex_
  //   - stripe mutexes in ascending cf id, ascending stripe order
  //   - wait_txn_map_mutex_
  //
  // Must be held when accessing/modifying lock_maps_.
  InstrumentedMutex lock_map_mutex_;

  // Map of ColumnFamilyId to locked key info
  using LockMaps = std::unordered_map<uint32_t, std::shared_ptr<LockMap>>;
  LockMaps lock_maps_;

  // Thread-local cache of entries in lock_maps_.  This is an optimization
  // to avoid acquiring a mutex in order to look up a LockMap
  std::unique_ptr<ThreadLocalPtr> lock_maps_cache_;

  // Must be held when modifying wait_txn_map_ and rev_wait_txn_map_.
  std::mutex wait_txn_map_mutex_;

  // Maps from waitee -> number of waiters.
  HashMap<TransactionID, int> rev_wait_txn_map_;
  // Maps from waiter -> waitee.
  HashMap<TransactionID, TrackedTrxInfo> wait_txn_map_;
  DeadlockInfoBuffer dlock_buffer_;

  // Used to allocate mutexes/condvars to use when locking keys
  std::shared_ptr<TransactionDBMutexFactory> mutex_factory_;

  bool IsLockExpired(TransactionID txn_id, const LockInfo& lock_info, Env* env,
                     uint64_t* wait_time);

  std::shared_ptr<LockMap> GetLockMap(uint32_t column_family_id);

  Status AcquireWithTimeout(PessimisticTransaction* txn, LockMap* lock_map,
                            LockMapStripe* stripe, uint32_t column_family_id,
                            const std::string& key, Env* env, int64_t timeout,
                            LockInfo&& lock_info);

  Status AcquireLocked(LockMap* lock_map, LockMapStripe* stripe,
                       const std::string& key, Env* env,
                       LockInfo&& lock_info, uint64_t* wait_time,
                       autovector<TransactionID>* txn_ids);

  void UnLockKey(PessimisticTransaction* txn, const std::string& key,
                 LockMapStripe* stripe, LockMap* lock_map, Env* env);

  bool IncrementWaiters(const PessimisticTransaction* txn,
                        const autovector<TransactionID>& wait_ids,
                        const std::string& key, const uint32_t& cf_id,
                        const bool& exclusive, Env* const env);
  void DecrementWaiters(const PessimisticTransaction* txn,
                        const autovector<TransactionID>& wait_ids);
  void DecrementWaitersImpl(const PessimisticTransaction* txn,
                            const autovector<TransactionID>& wait_ids);
};

}  // namespace CABINDB_NAMESPACE
#endif  // CABINDB_LITE
