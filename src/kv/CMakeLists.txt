set(kv_srcs
  KeyValueDB.cc
  MemDB.cc
  RocksDBStore.cc
  CabinDBStore.cc
  KeyValueHistogram.cc
  rocksdb_cache/ShardedCache.cc
  rocksdb_cache/BinnedLRUCache.cc
  cabindb_cache/ShardedCache.cc
  cabindb_cache/BinnedLRUCache.cc)

if (WITH_LEVELDB)
  list(APPEND kv_srcs LevelDBStore.cc)
endif (WITH_LEVELDB)

add_library(kv STATIC ${kv_srcs}
  $<TARGET_OBJECTS:common_prioritycache_obj>)

target_link_libraries(kv ${LEVELDB_LIBRARIES}
  cabindb
  RocksDB::RocksDB
  heap_profiler)
