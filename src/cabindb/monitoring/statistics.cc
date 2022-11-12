//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "monitoring/statistics.h"

#include <algorithm>
#include <cinttypes>
#include <cstdio>
#include "include/cabindb/statistics.h"

namespace CABINDB_NAMESPACE {

// The order of items listed in  Tickers should be the same as
// the order listed in TickersNameMap
const std::vector<std::pair<Tickers, std::string>> TickersNameMap = {
    {BLOCK_CACHE_MISS, "cabindb.block.cache.miss"},
    {BLOCK_CACHE_HIT, "cabindb.block.cache.hit"},
    {BLOCK_CACHE_ADD, "cabindb.block.cache.add"},
    {BLOCK_CACHE_ADD_FAILURES, "cabindb.block.cache.add.failures"},
    {BLOCK_CACHE_INDEX_MISS, "cabindb.block.cache.index.miss"},
    {BLOCK_CACHE_INDEX_HIT, "cabindb.block.cache.index.hit"},
    {BLOCK_CACHE_INDEX_ADD, "cabindb.block.cache.index.add"},
    {BLOCK_CACHE_INDEX_BYTES_INSERT, "cabindb.block.cache.index.bytes.insert"},
    {BLOCK_CACHE_INDEX_BYTES_EVICT, "cabindb.block.cache.index.bytes.evict"},
    {BLOCK_CACHE_FILTER_MISS, "cabindb.block.cache.filter.miss"},
    {BLOCK_CACHE_FILTER_HIT, "cabindb.block.cache.filter.hit"},
    {BLOCK_CACHE_FILTER_ADD, "cabindb.block.cache.filter.add"},
    {BLOCK_CACHE_FILTER_BYTES_INSERT,
     "cabindb.block.cache.filter.bytes.insert"},
    {BLOCK_CACHE_FILTER_BYTES_EVICT, "cabindb.block.cache.filter.bytes.evict"},
    {BLOCK_CACHE_DATA_MISS, "cabindb.block.cache.data.miss"},
    {BLOCK_CACHE_DATA_HIT, "cabindb.block.cache.data.hit"},
    {BLOCK_CACHE_DATA_ADD, "cabindb.block.cache.data.add"},
    {BLOCK_CACHE_DATA_BYTES_INSERT, "cabindb.block.cache.data.bytes.insert"},
    {BLOCK_CACHE_BYTES_READ, "cabindb.block.cache.bytes.read"},
    {BLOCK_CACHE_BYTES_WRITE, "cabindb.block.cache.bytes.write"},
    {BLOOM_FILTER_USEFUL, "cabindb.bloom.filter.useful"},
    {BLOOM_FILTER_FULL_POSITIVE, "cabindb.bloom.filter.full.positive"},
    {BLOOM_FILTER_FULL_TRUE_POSITIVE,
     "cabindb.bloom.filter.full.true.positive"},
    {BLOOM_FILTER_MICROS, "cabindb.bloom.filter.micros"},
    {PERSISTENT_CACHE_HIT, "cabindb.persistent.cache.hit"},
    {PERSISTENT_CACHE_MISS, "cabindb.persistent.cache.miss"},
    {SIM_BLOCK_CACHE_HIT, "cabindb.sim.block.cache.hit"},
    {SIM_BLOCK_CACHE_MISS, "cabindb.sim.block.cache.miss"},
    {MEMTABLE_HIT, "cabindb.memtable.hit"},
    {MEMTABLE_MISS, "cabindb.memtable.miss"},
    {GET_HIT_L0, "cabindb.l0.hit"},
    {GET_HIT_L1, "cabindb.l1.hit"},
    {GET_HIT_L2_AND_UP, "cabindb.l2andup.hit"},
    {COMPACTION_KEY_DROP_NEWER_ENTRY, "cabindb.compaction.key.drop.new"},
    {COMPACTION_KEY_DROP_OBSOLETE, "cabindb.compaction.key.drop.obsolete"},
    {COMPACTION_KEY_DROP_RANGE_DEL, "cabindb.compaction.key.drop.range_del"},
    {COMPACTION_KEY_DROP_USER, "cabindb.compaction.key.drop.user"},
    {COMPACTION_RANGE_DEL_DROP_OBSOLETE,
     "cabindb.compaction.range_del.drop.obsolete"},
    {COMPACTION_OPTIMIZED_DEL_DROP_OBSOLETE,
     "cabindb.compaction.optimized.del.drop.obsolete"},
    {COMPACTION_CANCELLED, "cabindb.compaction.cancelled"},
    {NUMBER_KEYS_WRITTEN, "cabindb.number.keys.written"},
    {NUMBER_KEYS_READ, "cabindb.number.keys.read"},
    {NUMBER_KEYS_UPDATED, "cabindb.number.keys.updated"},
    {BYTES_WRITTEN, "cabindb.bytes.written"},
    {BYTES_READ, "cabindb.bytes.read"},
    {NUMBER_DB_SEEK, "cabindb.number.db.seek"},
    {NUMBER_DB_NEXT, "cabindb.number.db.next"},
    {NUMBER_DB_PREV, "cabindb.number.db.prev"},
    {NUMBER_DB_SEEK_FOUND, "cabindb.number.db.seek.found"},
    {NUMBER_DB_NEXT_FOUND, "cabindb.number.db.next.found"},
    {NUMBER_DB_PREV_FOUND, "cabindb.number.db.prev.found"},
    {ITER_BYTES_READ, "cabindb.db.iter.bytes.read"},
    {NO_FILE_CLOSES, "cabindb.no.file.closes"},
    {NO_FILE_OPENS, "cabindb.no.file.opens"},
    {NO_FILE_ERRORS, "cabindb.no.file.errors"},
    {STALL_L0_SLOWDOWN_MICROS, "cabindb.l0.slowdown.micros"},
    {STALL_MEMTABLE_COMPACTION_MICROS, "cabindb.memtable.compaction.micros"},
    {STALL_L0_NUM_FILES_MICROS, "cabindb.l0.num.files.stall.micros"},
    {STALL_MICROS, "cabindb.stall.micros"},
    {DB_MUTEX_WAIT_MICROS, "cabindb.db.mutex.wait.micros"},
    {RATE_LIMIT_DELAY_MILLIS, "cabindb.rate.limit.delay.millis"},
    {NO_ITERATORS, "cabindb.num.iterators"},
    {NUMBER_MULTIGET_CALLS, "cabindb.number.multiget.get"},
    {NUMBER_MULTIGET_KEYS_READ, "cabindb.number.multiget.keys.read"},
    {NUMBER_MULTIGET_BYTES_READ, "cabindb.number.multiget.bytes.read"},
    {NUMBER_FILTERED_DELETES, "cabindb.number.deletes.filtered"},
    {NUMBER_MERGE_FAILURES, "cabindb.number.merge.failures"},
    {BLOOM_FILTER_PREFIX_CHECKED, "cabindb.bloom.filter.prefix.checked"},
    {BLOOM_FILTER_PREFIX_USEFUL, "cabindb.bloom.filter.prefix.useful"},
    {NUMBER_OF_RESEEKS_IN_ITERATION, "cabindb.number.reseeks.iteration"},
    {GET_UPDATES_SINCE_CALLS, "cabindb.getupdatessince.calls"},
    {BLOCK_CACHE_COMPRESSED_MISS, "cabindb.block.cachecompressed.miss"},
    {BLOCK_CACHE_COMPRESSED_HIT, "cabindb.block.cachecompressed.hit"},
    {BLOCK_CACHE_COMPRESSED_ADD, "cabindb.block.cachecompressed.add"},
    {BLOCK_CACHE_COMPRESSED_ADD_FAILURES,
     "cabindb.block.cachecompressed.add.failures"},
    {WAL_FILE_SYNCED, "cabindb.wal.synced"},
    {WAL_FILE_BYTES, "cabindb.wal.bytes"},
    {WRITE_DONE_BY_SELF, "cabindb.write.self"},
    {WRITE_DONE_BY_OTHER, "cabindb.write.other"},
    {WRITE_TIMEDOUT, "cabindb.write.timeout"},
    {WRITE_WITH_WAL, "cabindb.write.wal"},
    {COMPACT_READ_BYTES, "cabindb.compact.read.bytes"},
    {COMPACT_WRITE_BYTES, "cabindb.compact.write.bytes"},
    {FLUSH_WRITE_BYTES, "cabindb.flush.write.bytes"},
    {COMPACT_READ_BYTES_MARKED, "cabindb.compact.read.marked.bytes"},
    {COMPACT_READ_BYTES_PERIODIC, "cabindb.compact.read.periodic.bytes"},
    {COMPACT_READ_BYTES_TTL, "cabindb.compact.read.ttl.bytes"},
    {COMPACT_WRITE_BYTES_MARKED, "cabindb.compact.write.marked.bytes"},
    {COMPACT_WRITE_BYTES_PERIODIC, "cabindb.compact.write.periodic.bytes"},
    {COMPACT_WRITE_BYTES_TTL, "cabindb.compact.write.ttl.bytes"},
    {NUMBER_DIRECT_LOAD_TABLE_PROPERTIES,
     "cabindb.number.direct.load.table.properties"},
    {NUMBER_SUPERVERSION_ACQUIRES, "cabindb.number.superversion_acquires"},
    {NUMBER_SUPERVERSION_RELEASES, "cabindb.number.superversion_releases"},
    {NUMBER_SUPERVERSION_CLEANUPS, "cabindb.number.superversion_cleanups"},
    {NUMBER_BLOCK_COMPRESSED, "cabindb.number.block.compressed"},
    {NUMBER_BLOCK_DECOMPRESSED, "cabindb.number.block.decompressed"},
    {NUMBER_BLOCK_NOT_COMPRESSED, "cabindb.number.block.not_compressed"},
    {MERGE_OPERATION_TOTAL_TIME, "cabindb.merge.operation.time.nanos"},
    {FILTER_OPERATION_TOTAL_TIME, "cabindb.filter.operation.time.nanos"},
    {ROW_CACHE_HIT, "cabindb.row.cache.hit"},
    {ROW_CACHE_MISS, "cabindb.row.cache.miss"},
    {READ_AMP_ESTIMATE_USEFUL_BYTES, "cabindb.read.amp.estimate.useful.bytes"},
    {READ_AMP_TOTAL_READ_BYTES, "cabindb.read.amp.total.read.bytes"},
    {NUMBER_RATE_LIMITER_DRAINS, "cabindb.number.rate_limiter.drains"},
    {NUMBER_ITER_SKIP, "cabindb.number.iter.skip"},
    {BLOB_DB_NUM_PUT, "cabindb.blobdb.num.put"},
    {BLOB_DB_NUM_WRITE, "cabindb.blobdb.num.write"},
    {BLOB_DB_NUM_GET, "cabindb.blobdb.num.get"},
    {BLOB_DB_NUM_MULTIGET, "cabindb.blobdb.num.multiget"},
    {BLOB_DB_NUM_SEEK, "cabindb.blobdb.num.seek"},
    {BLOB_DB_NUM_NEXT, "cabindb.blobdb.num.next"},
    {BLOB_DB_NUM_PREV, "cabindb.blobdb.num.prev"},
    {BLOB_DB_NUM_KEYS_WRITTEN, "cabindb.blobdb.num.keys.written"},
    {BLOB_DB_NUM_KEYS_READ, "cabindb.blobdb.num.keys.read"},
    {BLOB_DB_BYTES_WRITTEN, "cabindb.blobdb.bytes.written"},
    {BLOB_DB_BYTES_READ, "cabindb.blobdb.bytes.read"},
    {BLOB_DB_WRITE_INLINED, "cabindb.blobdb.write.inlined"},
    {BLOB_DB_WRITE_INLINED_TTL, "cabindb.blobdb.write.inlined.ttl"},
    {BLOB_DB_WRITE_BLOB, "cabindb.blobdb.write.blob"},
    {BLOB_DB_WRITE_BLOB_TTL, "cabindb.blobdb.write.blob.ttl"},
    {BLOB_DB_BLOB_FILE_BYTES_WRITTEN, "cabindb.blobdb.blob.file.bytes.written"},
    {BLOB_DB_BLOB_FILE_BYTES_READ, "cabindb.blobdb.blob.file.bytes.read"},
    {BLOB_DB_BLOB_FILE_SYNCED, "cabindb.blobdb.blob.file.synced"},
    {BLOB_DB_BLOB_INDEX_EXPIRED_COUNT,
     "cabindb.blobdb.blob.index.expired.count"},
    {BLOB_DB_BLOB_INDEX_EXPIRED_SIZE, "cabindb.blobdb.blob.index.expired.size"},
    {BLOB_DB_BLOB_INDEX_EVICTED_COUNT,
     "cabindb.blobdb.blob.index.evicted.count"},
    {BLOB_DB_BLOB_INDEX_EVICTED_SIZE, "cabindb.blobdb.blob.index.evicted.size"},
    {BLOB_DB_GC_NUM_FILES, "cabindb.blobdb.gc.num.files"},
    {BLOB_DB_GC_NUM_NEW_FILES, "cabindb.blobdb.gc.num.new.files"},
    {BLOB_DB_GC_FAILURES, "cabindb.blobdb.gc.failures"},
    {BLOB_DB_GC_NUM_KEYS_OVERWRITTEN, "cabindb.blobdb.gc.num.keys.overwritten"},
    {BLOB_DB_GC_NUM_KEYS_EXPIRED, "cabindb.blobdb.gc.num.keys.expired"},
    {BLOB_DB_GC_NUM_KEYS_RELOCATED, "cabindb.blobdb.gc.num.keys.relocated"},
    {BLOB_DB_GC_BYTES_OVERWRITTEN, "cabindb.blobdb.gc.bytes.overwritten"},
    {BLOB_DB_GC_BYTES_EXPIRED, "cabindb.blobdb.gc.bytes.expired"},
    {BLOB_DB_GC_BYTES_RELOCATED, "cabindb.blobdb.gc.bytes.relocated"},
    {BLOB_DB_FIFO_NUM_FILES_EVICTED, "cabindb.blobdb.fifo.num.files.evicted"},
    {BLOB_DB_FIFO_NUM_KEYS_EVICTED, "cabindb.blobdb.fifo.num.keys.evicted"},
    {BLOB_DB_FIFO_BYTES_EVICTED, "cabindb.blobdb.fifo.bytes.evicted"},
    {TXN_PREPARE_MUTEX_OVERHEAD, "cabindb.txn.overhead.mutex.prepare"},
    {TXN_OLD_COMMIT_MAP_MUTEX_OVERHEAD,
     "cabindb.txn.overhead.mutex.old.commit.map"},
    {TXN_DUPLICATE_KEY_OVERHEAD, "cabindb.txn.overhead.duplicate.key"},
    {TXN_SNAPSHOT_MUTEX_OVERHEAD, "cabindb.txn.overhead.mutex.snapshot"},
    {TXN_GET_TRY_AGAIN, "cabindb.txn.get.tryagain"},
    {NUMBER_MULTIGET_KEYS_FOUND, "cabindb.number.multiget.keys.found"},
    {NO_ITERATOR_CREATED, "cabindb.num.iterator.created"},
    {NO_ITERATOR_DELETED, "cabindb.num.iterator.deleted"},
    {BLOCK_CACHE_COMPRESSION_DICT_MISS,
     "cabindb.block.cache.compression.dict.miss"},
    {BLOCK_CACHE_COMPRESSION_DICT_HIT,
     "cabindb.block.cache.compression.dict.hit"},
    {BLOCK_CACHE_COMPRESSION_DICT_ADD,
     "cabindb.block.cache.compression.dict.add"},
    {BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT,
     "cabindb.block.cache.compression.dict.bytes.insert"},
    {BLOCK_CACHE_COMPRESSION_DICT_BYTES_EVICT,
     "cabindb.block.cache.compression.dict.bytes.evict"},
    {BLOCK_CACHE_ADD_REDUNDANT, "cabindb.block.cache.add.redundant"},
    {BLOCK_CACHE_INDEX_ADD_REDUNDANT,
     "cabindb.block.cache.index.add.redundant"},
    {BLOCK_CACHE_FILTER_ADD_REDUNDANT,
     "cabindb.block.cache.filter.add.redundant"},
    {BLOCK_CACHE_DATA_ADD_REDUNDANT, "cabindb.block.cache.data.add.redundant"},
    {BLOCK_CACHE_COMPRESSION_DICT_ADD_REDUNDANT,
     "cabindb.block.cache.compression.dict.add.redundant"},
    {FILES_MARKED_TRASH, "cabindb.files.marked.trash"},
    {FILES_DELETED_IMMEDIATELY, "cabindb.files.deleted.immediately"},
};

const std::vector<std::pair<Histograms, std::string>> HistogramsNameMap = {
    {DB_GET, "cabindb.db.get.micros"},
    {DB_WRITE, "cabindb.db.write.micros"},
    {COMPACTION_TIME, "cabindb.compaction.times.micros"},
    {COMPACTION_CPU_TIME, "cabindb.compaction.times.cpu_micros"},
    {SUBCOMPACTION_SETUP_TIME, "cabindb.subcompaction.setup.times.micros"},
    {TABLE_SYNC_MICROS, "cabindb.table.sync.micros"},
    {COMPACTION_OUTFILE_SYNC_MICROS, "cabindb.compaction.outfile.sync.micros"},
    {WAL_FILE_SYNC_MICROS, "cabindb.wal.file.sync.micros"},
    {MANIFEST_FILE_SYNC_MICROS, "cabindb.manifest.file.sync.micros"},
    {TABLE_OPEN_IO_MICROS, "cabindb.table.open.io.micros"},
    {DB_MULTIGET, "cabindb.db.multiget.micros"},
    {READ_BLOCK_COMPACTION_MICROS, "cabindb.read.block.compaction.micros"},
    {READ_BLOCK_GET_MICROS, "cabindb.read.block.get.micros"},
    {WRITE_RAW_BLOCK_MICROS, "cabindb.write.raw.block.micros"},
    {STALL_L0_SLOWDOWN_COUNT, "cabindb.l0.slowdown.count"},
    {STALL_MEMTABLE_COMPACTION_COUNT, "cabindb.memtable.compaction.count"},
    {STALL_L0_NUM_FILES_COUNT, "cabindb.num.files.stall.count"},
    {HARD_RATE_LIMIT_DELAY_COUNT, "cabindb.hard.rate.limit.delay.count"},
    {SOFT_RATE_LIMIT_DELAY_COUNT, "cabindb.soft.rate.limit.delay.count"},
    {NUM_FILES_IN_SINGLE_COMPACTION, "cabindb.numfiles.in.singlecompaction"},
    {DB_SEEK, "cabindb.db.seek.micros"},
    {WRITE_STALL, "cabindb.db.write.stall"},
    {SST_READ_MICROS, "cabindb.sst.read.micros"},
    {NUM_SUBCOMPACTIONS_SCHEDULED, "cabindb.num.subcompactions.scheduled"},
    {BYTES_PER_READ, "cabindb.bytes.per.read"},
    {BYTES_PER_WRITE, "cabindb.bytes.per.write"},
    {BYTES_PER_MULTIGET, "cabindb.bytes.per.multiget"},
    {BYTES_COMPRESSED, "cabindb.bytes.compressed"},
    {BYTES_DECOMPRESSED, "cabindb.bytes.decompressed"},
    {COMPRESSION_TIMES_NANOS, "cabindb.compression.times.nanos"},
    {DECOMPRESSION_TIMES_NANOS, "cabindb.decompression.times.nanos"},
    {READ_NUM_MERGE_OPERANDS, "cabindb.read.num.merge_operands"},
    {BLOB_DB_KEY_SIZE, "cabindb.blobdb.key.size"},
    {BLOB_DB_VALUE_SIZE, "cabindb.blobdb.value.size"},
    {BLOB_DB_WRITE_MICROS, "cabindb.blobdb.write.micros"},
    {BLOB_DB_GET_MICROS, "cabindb.blobdb.get.micros"},
    {BLOB_DB_MULTIGET_MICROS, "cabindb.blobdb.multiget.micros"},
    {BLOB_DB_SEEK_MICROS, "cabindb.blobdb.seek.micros"},
    {BLOB_DB_NEXT_MICROS, "cabindb.blobdb.next.micros"},
    {BLOB_DB_PREV_MICROS, "cabindb.blobdb.prev.micros"},
    {BLOB_DB_BLOB_FILE_WRITE_MICROS, "cabindb.blobdb.blob.file.write.micros"},
    {BLOB_DB_BLOB_FILE_READ_MICROS, "cabindb.blobdb.blob.file.read.micros"},
    {BLOB_DB_BLOB_FILE_SYNC_MICROS, "cabindb.blobdb.blob.file.sync.micros"},
    {BLOB_DB_GC_MICROS, "cabindb.blobdb.gc.micros"},
    {BLOB_DB_COMPRESSION_MICROS, "cabindb.blobdb.compression.micros"},
    {BLOB_DB_DECOMPRESSION_MICROS, "cabindb.blobdb.decompression.micros"},
    {FLUSH_TIME, "cabindb.db.flush.micros"},
    {SST_BATCH_SIZE, "cabindb.sst.batch.size"},
    {NUM_INDEX_AND_FILTER_BLOCKS_READ_PER_LEVEL,
     "cabindb.num.index.and.filter.blocks.read.per.level"},
    {NUM_DATA_BLOCKS_READ_PER_LEVEL, "cabindb.num.data.blocks.read.per.level"},
    {NUM_SST_READ_PER_LEVEL, "cabindb.num.sst.read.per.level"},
};

std::shared_ptr<Statistics> CreateDBStatistics() {
  return std::make_shared<StatisticsImpl>(nullptr);
}

StatisticsImpl::StatisticsImpl(std::shared_ptr<Statistics> stats)
    : stats_(std::move(stats)) {}

StatisticsImpl::~StatisticsImpl() {}

uint64_t StatisticsImpl::getTickerCount(uint32_t tickerType) const {
  MutexLock lock(&aggregate_lock_);
  return getTickerCountLocked(tickerType);
}

uint64_t StatisticsImpl::getTickerCountLocked(uint32_t tickerType) const {
  assert(tickerType < TICKER_ENUM_MAX);
  uint64_t res = 0;
  for (size_t core_idx = 0; core_idx < per_core_stats_.Size(); ++core_idx) {
    res += per_core_stats_.AccessAtCore(core_idx)->tickers_[tickerType];
  }
  return res;
}

void StatisticsImpl::histogramData(uint32_t histogramType,
                                   HistogramData* const data) const {
  MutexLock lock(&aggregate_lock_);
  getHistogramImplLocked(histogramType)->Data(data);
}

std::unique_ptr<HistogramImpl> StatisticsImpl::getHistogramImplLocked(
    uint32_t histogramType) const {
  assert(histogramType < HISTOGRAM_ENUM_MAX);
  std::unique_ptr<HistogramImpl> res_hist(new HistogramImpl());
  for (size_t core_idx = 0; core_idx < per_core_stats_.Size(); ++core_idx) {
    res_hist->Merge(
        per_core_stats_.AccessAtCore(core_idx)->histograms_[histogramType]);
  }
  return res_hist;
}

std::string StatisticsImpl::getHistogramString(uint32_t histogramType) const {
  MutexLock lock(&aggregate_lock_);
  return getHistogramImplLocked(histogramType)->ToString();
}

void StatisticsImpl::setTickerCount(uint32_t tickerType, uint64_t count) {
  {
    MutexLock lock(&aggregate_lock_);
    setTickerCountLocked(tickerType, count);
  }
  if (stats_ && tickerType < TICKER_ENUM_MAX) {
    stats_->setTickerCount(tickerType, count);
  }
}

void StatisticsImpl::setTickerCountLocked(uint32_t tickerType, uint64_t count) {
  assert(tickerType < TICKER_ENUM_MAX);
  for (size_t core_idx = 0; core_idx < per_core_stats_.Size(); ++core_idx) {
    if (core_idx == 0) {
      per_core_stats_.AccessAtCore(core_idx)->tickers_[tickerType] = count;
    } else {
      per_core_stats_.AccessAtCore(core_idx)->tickers_[tickerType] = 0;
    }
  }
}

uint64_t StatisticsImpl::getAndResetTickerCount(uint32_t tickerType) {
  uint64_t sum = 0;
  {
    MutexLock lock(&aggregate_lock_);
    assert(tickerType < TICKER_ENUM_MAX);
    for (size_t core_idx = 0; core_idx < per_core_stats_.Size(); ++core_idx) {
      sum +=
          per_core_stats_.AccessAtCore(core_idx)->tickers_[tickerType].exchange(
              0, std::memory_order_relaxed);
    }
  }
  if (stats_ && tickerType < TICKER_ENUM_MAX) {
    stats_->setTickerCount(tickerType, 0);
  }
  return sum;
}

void StatisticsImpl::recordTick(uint32_t tickerType, uint64_t count) {
  if (get_stats_level() <= StatsLevel::kExceptTickers) {
    return;
  }
  if (tickerType < TICKER_ENUM_MAX) {
    per_core_stats_.Access()->tickers_[tickerType].fetch_add(
        count, std::memory_order_relaxed);
    if (stats_) {
      stats_->recordTick(tickerType, count);
    }
  } else {
    assert(false);
  }
}

void StatisticsImpl::recordInHistogram(uint32_t histogramType, uint64_t value) {
  assert(histogramType < HISTOGRAM_ENUM_MAX);
  if (get_stats_level() <= StatsLevel::kExceptHistogramOrTimers) {
    return;
  }
  per_core_stats_.Access()->histograms_[histogramType].Add(value);
  if (stats_ && histogramType < HISTOGRAM_ENUM_MAX) {
    stats_->recordInHistogram(histogramType, value);
  }
}

Status StatisticsImpl::Reset() {
  MutexLock lock(&aggregate_lock_);
  for (uint32_t i = 0; i < TICKER_ENUM_MAX; ++i) {
    setTickerCountLocked(i, 0);
  }
  for (uint32_t i = 0; i < HISTOGRAM_ENUM_MAX; ++i) {
    for (size_t core_idx = 0; core_idx < per_core_stats_.Size(); ++core_idx) {
      per_core_stats_.AccessAtCore(core_idx)->histograms_[i].Clear();
    }
  }
  return Status::OK();
}

namespace {

// a buffer size used for temp string buffers
const int kTmpStrBufferSize = 200;

} // namespace

std::string StatisticsImpl::ToString() const {
  MutexLock lock(&aggregate_lock_);
  std::string res;
  res.reserve(20000);
  for (const auto& t : TickersNameMap) {
    assert(t.first < TICKER_ENUM_MAX);
    char buffer[kTmpStrBufferSize];
    snprintf(buffer, kTmpStrBufferSize, "%s COUNT : %" PRIu64 "\n",
             t.second.c_str(), getTickerCountLocked(t.first));
    res.append(buffer);
  }
  for (const auto& h : HistogramsNameMap) {
    assert(h.first < HISTOGRAM_ENUM_MAX);
    char buffer[kTmpStrBufferSize];
    HistogramData hData;
    getHistogramImplLocked(h.first)->Data(&hData);
    // don't handle failures - buffer should always be big enough and arguments
    // should be provided correctly
    int ret =
        snprintf(buffer, kTmpStrBufferSize,
                 "%s P50 : %f P95 : %f P99 : %f P100 : %f COUNT : %" PRIu64
                 " SUM : %" PRIu64 "\n",
                 h.second.c_str(), hData.median, hData.percentile95,
                 hData.percentile99, hData.max, hData.count, hData.sum);
    if (ret < 0 || ret >= kTmpStrBufferSize) {
      assert(false);
      continue;
    }
    res.append(buffer);
  }
  res.shrink_to_fit();
  return res;
}

bool StatisticsImpl::getTickerMap(
    std::map<std::string, uint64_t>* stats_map) const {
  assert(stats_map);
  if (!stats_map) return false;
  stats_map->clear();
  MutexLock lock(&aggregate_lock_);
  for (const auto& t : TickersNameMap) {
    assert(t.first < TICKER_ENUM_MAX);
    (*stats_map)[t.second.c_str()] = getTickerCountLocked(t.first);
  }
  return true;
}

bool StatisticsImpl::HistEnabledForType(uint32_t type) const {
  return type < HISTOGRAM_ENUM_MAX;
}

}  // namespace CABINDB_NAMESPACE
