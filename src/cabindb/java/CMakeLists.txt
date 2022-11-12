cmake_minimum_required(VERSION 3.4)

if(${CMAKE_VERSION} VERSION_LESS "3.11.4")
    message("Please consider switching to CMake 3.11.4 or newer")
endif()

set(CMAKE_JAVA_COMPILE_FLAGS -source 7)

set(JNI_NATIVE_SOURCES
        cabinjni/backupablejni.cc
        cabinjni/backupenginejni.cc
        cabinjni/cassandra_compactionfilterjni.cc
        cabinjni/cassandra_value_operator.cc
        cabinjni/checkpoint.cc
        cabinjni/clock_cache.cc
        cabinjni/columnfamilyhandle.cc
        cabinjni/compaction_filter.cc
        cabinjni/compaction_filter_factory.cc
        cabinjni/compaction_filter_factory_jnicallback.cc
        cabinjni/compaction_job_info.cc
        cabinjni/compaction_job_stats.cc
        cabinjni/compaction_options.cc
        cabinjni/compaction_options_fifo.cc
        cabinjni/compaction_options_universal.cc
        cabinjni/compact_range_options.cc
        cabinjni/comparator.cc
        cabinjni/comparatorjnicallback.cc
        cabinjni/compression_options.cc
        cabinjni/concurrent_task_limiter.cc
        cabinjni/config_options.cc
        cabinjni/env.cc
        cabinjni/env_options.cc
        cabinjni/event_listener.cc
        cabinjni/event_listener_jnicallback.cc
        cabinjni/filter.cc
        cabinjni/ingest_external_file_options.cc
        cabinjni/iterator.cc
        cabinjni/jnicallback.cc
        cabinjni/loggerjnicallback.cc
        cabinjni/lru_cache.cc
        cabinjni/memory_util.cc
        cabinjni/memtablejni.cc
        cabinjni/merge_operator.cc
        cabinjni/native_comparator_wrapper_test.cc
        cabinjni/optimistic_transaction_db.cc
        cabinjni/optimistic_transaction_options.cc
        cabinjni/options.cc
        cabinjni/options_util.cc
        cabinjni/persistent_cache.cc
        cabinjni/ratelimiterjni.cc
        cabinjni/remove_emptyvalue_compactionfilterjni.cc
        cabinjni/restorejni.cc
        cabinjni/cabin_callback_object.cc
        cabinjni/cabindb_exception_test.cc
        cabinjni/cabinjni.cc
        cabinjni/slice.cc
        cabinjni/snapshot.cc
        cabinjni/sst_file_manager.cc
        cabinjni/sst_file_writerjni.cc
        cabinjni/sst_file_readerjni.cc
        cabinjni/sst_file_reader_iterator.cc
        cabinjni/sst_partitioner.cc
        cabinjni/statistics.cc
        cabinjni/statisticsjni.cc
        cabinjni/table.cc
        cabinjni/table_filter.cc
        cabinjni/table_filter_jnicallback.cc
        cabinjni/thread_status.cc
        cabinjni/trace_writer.cc
        cabinjni/trace_writer_jnicallback.cc
        cabinjni/transaction.cc
        cabinjni/transaction_db.cc
        cabinjni/transaction_db_options.cc
        cabinjni/transaction_log.cc
        cabinjni/transaction_notifier.cc
        cabinjni/transaction_notifier_jnicallback.cc
        cabinjni/transaction_options.cc
        cabinjni/ttl.cc
        cabinjni/wal_filter.cc
        cabinjni/wal_filter_jnicallback.cc
        cabinjni/write_batch.cc
        cabinjni/writebatchhandlerjnicallback.cc
        cabinjni/write_batch_test.cc
        cabinjni/write_batch_with_index.cc
        cabinjni/write_buffer_manager.cc
)

set(JAVA_MAIN_CLASSES
  src/main/java/org/cabindb/AbstractCompactionFilter.java
  src/main/java/org/cabindb/AbstractCompactionFilterFactory.java
  src/main/java/org/cabindb/AbstractComparator.java
  src/main/java/org/cabindb/AbstractEventListener.java
  src/main/java/org/cabindb/AbstractImmutableNativeReference.java
  src/main/java/org/cabindb/AbstractMutableOptions.java
  src/main/java/org/cabindb/AbstractNativeReference.java
  src/main/java/org/cabindb/AbstractCabinIterator.java
  src/main/java/org/cabindb/AbstractSlice.java
  src/main/java/org/cabindb/AbstractTableFilter.java
  src/main/java/org/cabindb/AbstractTraceWriter.java
  src/main/java/org/cabindb/AbstractTransactionNotifier.java
  src/main/java/org/cabindb/AbstractWalFilter.java
  src/main/java/org/cabindb/AbstractWriteBatch.java
  src/main/java/org/cabindb/AccessHint.java
  src/main/java/org/cabindb/AdvancedColumnFamilyOptionsInterface.java
  src/main/java/org/cabindb/AdvancedMutableColumnFamilyOptionsInterface.java
  src/main/java/org/cabindb/BackgroundErrorReason.java
  src/main/java/org/cabindb/BackupableDBOptions.java
  src/main/java/org/cabindb/BackupEngine.java
  src/main/java/org/cabindb/BackupInfo.java
  src/main/java/org/cabindb/BlockBasedTableConfig.java
  src/main/java/org/cabindb/BloomFilter.java
  src/main/java/org/cabindb/BuiltinComparator.java
  src/main/java/org/cabindb/Cache.java
  src/main/java/org/cabindb/CassandraCompactionFilter.java
  src/main/java/org/cabindb/CassandraValueMergeOperator.java
  src/main/java/org/cabindb/Checkpoint.java
  src/main/java/org/cabindb/ChecksumType.java
  src/main/java/org/cabindb/ClockCache.java
  src/main/java/org/cabindb/ColumnFamilyDescriptor.java
  src/main/java/org/cabindb/ColumnFamilyHandle.java
  src/main/java/org/cabindb/ColumnFamilyMetaData.java
  src/main/java/org/cabindb/ColumnFamilyOptionsInterface.java
  src/main/java/org/cabindb/ColumnFamilyOptions.java
  src/main/java/org/cabindb/CompactionJobInfo.java
  src/main/java/org/cabindb/CompactionJobStats.java
  src/main/java/org/cabindb/CompactionOptions.java
  src/main/java/org/cabindb/CompactionOptionsFIFO.java
  src/main/java/org/cabindb/CompactionOptionsUniversal.java
  src/main/java/org/cabindb/CompactionPriority.java
  src/main/java/org/cabindb/CompactionReason.java
  src/main/java/org/cabindb/CompactRangeOptions.java
  src/main/java/org/cabindb/CompactionStopStyle.java
  src/main/java/org/cabindb/CompactionStyle.java
  src/main/java/org/cabindb/ComparatorOptions.java
  src/main/java/org/cabindb/ComparatorType.java
  src/main/java/org/cabindb/CompressionOptions.java
  src/main/java/org/cabindb/CompressionType.java
  src/main/java/org/cabindb/ConfigOptions.java
  src/main/java/org/cabindb/DataBlockIndexType.java
  src/main/java/org/cabindb/DBOptionsInterface.java
  src/main/java/org/cabindb/DBOptions.java
  src/main/java/org/cabindb/DbPath.java
  src/main/java/org/cabindb/DirectSlice.java
  src/main/java/org/cabindb/EncodingType.java
  src/main/java/org/cabindb/Env.java
  src/main/java/org/cabindb/EnvOptions.java
  src/main/java/org/cabindb/EventListener.java
  src/main/java/org/cabindb/Experimental.java
  src/main/java/org/cabindb/ExternalFileIngestionInfo.java
  src/main/java/org/cabindb/Filter.java
  src/main/java/org/cabindb/FileOperationInfo.java
  src/main/java/org/cabindb/FlushJobInfo.java
  src/main/java/org/cabindb/FlushReason.java
  src/main/java/org/cabindb/FlushOptions.java
  src/main/java/org/cabindb/HashLinkedListMemTableConfig.java
  src/main/java/org/cabindb/HashSkipListMemTableConfig.java
  src/main/java/org/cabindb/HdfsEnv.java
  src/main/java/org/cabindb/HistogramData.java
  src/main/java/org/cabindb/HistogramType.java
  src/main/java/org/cabindb/Holder.java
  src/main/java/org/cabindb/IndexShorteningMode.java
  src/main/java/org/cabindb/IndexType.java
  src/main/java/org/cabindb/InfoLogLevel.java
  src/main/java/org/cabindb/IngestExternalFileOptions.java
  src/main/java/org/cabindb/LevelMetaData.java
  src/main/java/org/cabindb/ConcurrentTaskLimiter.java
  src/main/java/org/cabindb/ConcurrentTaskLimiterImpl.java
  src/main/java/org/cabindb/LiveFileMetaData.java
  src/main/java/org/cabindb/LogFile.java
  src/main/java/org/cabindb/Logger.java
  src/main/java/org/cabindb/LRUCache.java
  src/main/java/org/cabindb/MemoryUsageType.java
  src/main/java/org/cabindb/MemoryUtil.java
  src/main/java/org/cabindb/MemTableConfig.java
  src/main/java/org/cabindb/MemTableInfo.java
  src/main/java/org/cabindb/MergeOperator.java
  src/main/java/org/cabindb/MutableColumnFamilyOptions.java
  src/main/java/org/cabindb/MutableColumnFamilyOptionsInterface.java
  src/main/java/org/cabindb/MutableDBOptions.java
  src/main/java/org/cabindb/MutableDBOptionsInterface.java
  src/main/java/org/cabindb/MutableOptionKey.java
  src/main/java/org/cabindb/MutableOptionValue.java
  src/main/java/org/cabindb/NativeComparatorWrapper.java
  src/main/java/org/cabindb/NativeLibraryLoader.java
  src/main/java/org/cabindb/OperationStage.java
  src/main/java/org/cabindb/OperationType.java
  src/main/java/org/cabindb/OptimisticTransactionDB.java
  src/main/java/org/cabindb/OptimisticTransactionOptions.java
  src/main/java/org/cabindb/Options.java
  src/main/java/org/cabindb/OptionsUtil.java
  src/main/java/org/cabindb/PersistentCache.java
  src/main/java/org/cabindb/PlainTableConfig.java
  src/main/java/org/cabindb/Priority.java
  src/main/java/org/cabindb/Range.java
  src/main/java/org/cabindb/RateLimiter.java
  src/main/java/org/cabindb/RateLimiterMode.java
  src/main/java/org/cabindb/ReadOptions.java
  src/main/java/org/cabindb/ReadTier.java
  src/main/java/org/cabindb/RemoveEmptyValueCompactionFilter.java
  src/main/java/org/cabindb/RestoreOptions.java
  src/main/java/org/cabindb/ReusedSynchronisationType.java
  src/main/java/org/cabindb/CabinCallbackObject.java
  src/main/java/org/cabindb/CabinDBException.java
  src/main/java/org/cabindb/CabinDB.java
  src/main/java/org/cabindb/CabinEnv.java
  src/main/java/org/cabindb/CabinIteratorInterface.java
  src/main/java/org/cabindb/CabinIterator.java
  src/main/java/org/cabindb/CabinMemEnv.java
  src/main/java/org/cabindb/CabinMutableObject.java
  src/main/java/org/cabindb/CabinObject.java
  src/main/java/org/cabindb/SanityLevel.java
  src/main/java/org/cabindb/SizeApproximationFlag.java
  src/main/java/org/cabindb/SkipListMemTableConfig.java
  src/main/java/org/cabindb/Slice.java
  src/main/java/org/cabindb/Snapshot.java
  src/main/java/org/cabindb/SstFileManager.java
  src/main/java/org/cabindb/SstFileMetaData.java
  src/main/java/org/cabindb/SstFileReader.java
  src/main/java/org/cabindb/SstFileReaderIterator.java
  src/main/java/org/cabindb/SstFileWriter.java
  src/main/java/org/cabindb/SstPartitionerFactory.java
  src/main/java/org/cabindb/SstPartitionerFixedPrefixFactory.java
  src/main/java/org/cabindb/StateType.java
  src/main/java/org/cabindb/StatisticsCollectorCallback.java
  src/main/java/org/cabindb/StatisticsCollector.java
  src/main/java/org/cabindb/Statistics.java
  src/main/java/org/cabindb/StatsCollectorInput.java
  src/main/java/org/cabindb/StatsLevel.java
  src/main/java/org/cabindb/Status.java
  src/main/java/org/cabindb/StringAppendOperator.java
  src/main/java/org/cabindb/TableFileCreationBriefInfo.java
  src/main/java/org/cabindb/TableFileCreationInfo.java
  src/main/java/org/cabindb/TableFileCreationReason.java
  src/main/java/org/cabindb/TableFileDeletionInfo.java
  src/main/java/org/cabindb/TableFilter.java
  src/main/java/org/cabindb/TableProperties.java
  src/main/java/org/cabindb/TableFormatConfig.java
  src/main/java/org/cabindb/ThreadType.java
  src/main/java/org/cabindb/ThreadStatus.java
  src/main/java/org/cabindb/TickerType.java
  src/main/java/org/cabindb/TimedEnv.java
  src/main/java/org/cabindb/TraceOptions.java
  src/main/java/org/cabindb/TraceWriter.java
  src/main/java/org/cabindb/TransactionalDB.java
  src/main/java/org/cabindb/TransactionalOptions.java
  src/main/java/org/cabindb/TransactionDB.java
  src/main/java/org/cabindb/TransactionDBOptions.java
  src/main/java/org/cabindb/Transaction.java
  src/main/java/org/cabindb/TransactionLogIterator.java
  src/main/java/org/cabindb/TransactionOptions.java
  src/main/java/org/cabindb/TtlDB.java
  src/main/java/org/cabindb/TxnDBWritePolicy.java
  src/main/java/org/cabindb/VectorMemTableConfig.java
  src/main/java/org/cabindb/WalFileType.java
  src/main/java/org/cabindb/WalFilter.java
  src/main/java/org/cabindb/WalProcessingOption.java
  src/main/java/org/cabindb/WALRecoveryMode.java
  src/main/java/org/cabindb/WBWICabinIterator.java
  src/main/java/org/cabindb/WriteBatch.java
  src/main/java/org/cabindb/WriteBatchInterface.java
  src/main/java/org/cabindb/WriteBatchWithIndex.java
  src/main/java/org/cabindb/WriteOptions.java
  src/main/java/org/cabindb/WriteBufferManager.java
  src/main/java/org/cabindb/WriteStallCondition.java
  src/main/java/org/cabindb/WriteStallInfo.java
  src/main/java/org/cabindb/util/ByteUtil.java
  src/main/java/org/cabindb/util/BytewiseComparator.java
  src/main/java/org/cabindb/util/Environment.java
  src/main/java/org/cabindb/util/IntComparator.java
  src/main/java/org/cabindb/util/ReverseBytewiseComparator.java
  src/main/java/org/cabindb/util/SizeUnit.java
  src/main/java/org/cabindb/UInt64AddOperator.java
)

set(JAVA_TEST_CLASSES
  src/test/java/org/cabindb/BackupEngineTest.java
  src/test/java/org/cabindb/IngestExternalFileOptionsTest.java
  src/test/java/org/cabindb/NativeComparatorWrapperTest.java
  src/test/java/org/cabindb/PlatformRandomHelper.java
  src/test/java/org/cabindb/CabinDBExceptionTest.java
  src/test/java/org/cabindb/CabinNativeLibraryResource.java
  src/test/java/org/cabindb/SnapshotTest.java
  src/test/java/org/cabindb/WriteBatchTest.java
  src/test/java/org/cabindb/util/CapturingWriteBatchHandler.java
  src/test/java/org/cabindb/util/WriteBatchGetter.java
)

include(FindJava)
include(UseJava)
find_package(JNI)

include_directories(${JNI_INCLUDE_DIRS})
include_directories(${PROJECT_SOURCE_DIR}/java)

set(JAVA_TEST_LIBDIR ${PROJECT_SOURCE_DIR}/java/test-libs)
set(JAVA_TMP_JAR ${JAVA_TEST_LIBDIR}/tmp.jar)
set(JAVA_JUNIT_JAR ${JAVA_TEST_LIBDIR}/junit-4.12.jar)
set(JAVA_HAMCR_JAR ${JAVA_TEST_LIBDIR}/hamcrest-core-1.3.jar)
set(JAVA_MOCKITO_JAR ${JAVA_TEST_LIBDIR}/mockito-all-1.10.19.jar)
set(JAVA_CGLIB_JAR ${JAVA_TEST_LIBDIR}/cglib-2.2.2.jar)
set(JAVA_ASSERTJ_JAR ${JAVA_TEST_LIBDIR}/assertj-core-1.7.1.jar)
set(JAVA_TESTCLASSPATH ${JAVA_JUNIT_JAR} ${JAVA_HAMCR_JAR} ${JAVA_MOCKITO_JAR} ${JAVA_CGLIB_JAR} ${JAVA_ASSERTJ_JAR})

set(JNI_OUTPUT_DIR ${PROJECT_SOURCE_DIR}/java/include)
file(MAKE_DIRECTORY ${JNI_OUTPUT_DIR})

if(${Java_VERSION_MAJOR} VERSION_GREATER_EQUAL "10" AND ${CMAKE_VERSION} VERSION_LESS "3.11.4")
  # Java 10 and newer don't have javah, but the alternative GENERATE_NATIVE_HEADERS requires CMake 3.11.4 or newer
  message(FATAL_ERROR "Detected Java 10 or newer (${Java_VERSION_STRING}), to build with CMake please upgrade CMake to 3.11.4 or newer")

elseif(${CMAKE_VERSION} VERSION_LESS "3.11.4" OR (${Java_VERSION_MINOR} STREQUAL "7" AND ${Java_VERSION_MAJOR} STREQUAL "1"))
  # Old CMake or Java 1.7 prepare the JAR...
  message("Preparing Jar for Java 7")
  add_jar(
      cabindbjni_classes
      SOURCES
      ${JAVA_MAIN_CLASSES}
      ${JAVA_TEST_CLASSES}
      INCLUDE_JARS ${JAVA_TESTCLASSPATH}
  )

else ()
  # Java 1.8 or newer prepare the JAR...
  message("Preparing Jar for JDK ${Java_VERSION_STRING}")
  add_jar(
      cabindbjni_classes
      SOURCES
      ${JAVA_MAIN_CLASSES}
      ${JAVA_TEST_CLASSES}
      INCLUDE_JARS ${JAVA_TESTCLASSPATH}
      GENERATE_NATIVE_HEADERS cabindbjni_headers DESTINATION ${JNI_OUTPUT_DIR}
  )

endif()

if(NOT EXISTS ${PROJECT_SOURCE_DIR}/java/classes)
  file(MAKE_DIRECTORY ${PROJECT_SOURCE_DIR}/java/classes)
endif()

if(NOT EXISTS ${JAVA_TEST_LIBDIR})
  file(MAKE_DIRECTORY mkdir ${JAVA_TEST_LIBDIR})
endif()

if (DEFINED CUSTOM_DEPS_URL)
  set(DEPS_URL ${CUSTOM_DEPS_URL}/)
else ()
  # Using a Facebook AWS account for S3 storage. (maven.org has a history
  # of failing in Travis builds.)
  set(DEPS_URL "https://cabindb-deps.s3-us-west-2.amazonaws.com/jars")
endif()

if(NOT EXISTS ${JAVA_JUNIT_JAR})
  message("Downloading ${JAVA_JUNIT_JAR}")
  file(DOWNLOAD ${DEPS_URL}/junit-4.12.jar ${JAVA_TMP_JAR} STATUS downloadStatus)
  list(GET downloadStatus 0 error_code)
  list(GET downloadStatus 1 error_message)
  if(NOT error_code EQUAL 0)
    message(FATAL_ERROR "Failed downloading ${JAVA_JUNIT_JAR}: ${error_message}")
  endif()
  file(RENAME ${JAVA_TMP_JAR} ${JAVA_JUNIT_JAR})
endif()
if(NOT EXISTS ${JAVA_HAMCR_JAR})
  message("Downloading ${JAVA_HAMCR_JAR}")
  file(DOWNLOAD ${DEPS_URL}/hamcrest-core-1.3.jar ${JAVA_TMP_JAR} STATUS downloadStatus)
  list(GET downloadStatus 0 error_code)
  list(GET downloadStatus 1 error_message)
  if(NOT error_code EQUAL 0)
    message(FATAL_ERROR "Failed downloading ${JAVA_HAMCR_JAR}: ${error_message}")
  endif()
  file(RENAME ${JAVA_TMP_JAR} ${JAVA_HAMCR_JAR})
endif()
if(NOT EXISTS ${JAVA_MOCKITO_JAR})
  message("Downloading ${JAVA_MOCKITO_JAR}")
  file(DOWNLOAD ${DEPS_URL}/mockito-all-1.10.19.jar ${JAVA_TMP_JAR} STATUS downloadStatus)
  list(GET downloadStatus 0 error_code)
  list(GET downloadStatus 1 error_message)
  if(NOT error_code EQUAL 0)
    message(FATAL_ERROR "Failed downloading ${JAVA_MOCKITO_JAR}: ${error_message}")
  endif()
  file(RENAME ${JAVA_TMP_JAR} ${JAVA_MOCKITO_JAR})
endif()
if(NOT EXISTS ${JAVA_CGLIB_JAR})
  message("Downloading ${JAVA_CGLIB_JAR}")
  file(DOWNLOAD ${DEPS_URL}/cglib-2.2.2.jar ${JAVA_TMP_JAR} STATUS downloadStatus)
  list(GET downloadStatus 0 error_code)
  list(GET downloadStatus 1 error_message)
  if(NOT error_code EQUAL 0)
    message(FATAL_ERROR "Failed downloading ${JAVA_CGLIB_JAR}: ${error_message}")
  endif()
  file(RENAME ${JAVA_TMP_JAR} ${JAVA_CGLIB_JAR})
endif()
if(NOT EXISTS ${JAVA_ASSERTJ_JAR})
  message("Downloading ${JAVA_ASSERTJ_JAR}")
  file(DOWNLOAD ${DEPS_URL}/assertj-core-1.7.1.jar ${JAVA_TMP_JAR} STATUS downloadStatus)
  list(GET downloadStatus 0 error_code)
  list(GET downloadStatus 1 error_message)
  if(NOT error_code EQUAL 0)
    message(FATAL_ERROR "Failed downloading ${JAVA_ASSERTJ_JAR}: ${error_message}")
  endif()
  file(RENAME ${JAVA_TMP_JAR} ${JAVA_ASSERTJ_JAR})
endif()

if(${CMAKE_VERSION} VERSION_LESS "3.11.4" OR (${Java_VERSION_MINOR} STREQUAL "7" AND ${Java_VERSION_MAJOR} STREQUAL "1"))
  # Old CMake or Java 1.7 ONLY generate JNI headers, Java 1.8+ JNI is handled in add_jar step above
  message("Preparing JNI headers for Java 7")
  set(NATIVE_JAVA_CLASSES
          org.cabindb.AbstractCompactionFilter
          org.cabindb.AbstractCompactionFilterFactory
          org.cabindb.AbstractComparator
          org.cabindb.AbstractEventListener
          org.cabindb.AbstractImmutableNativeReference
          org.cabindb.AbstractNativeReference
          org.cabindb.AbstractCabinIterator
          org.cabindb.AbstractSlice
          org.cabindb.AbstractTableFilter
          org.cabindb.AbstractTraceWriter
          org.cabindb.AbstractTransactionNotifier
          org.cabindb.AbstractWalFilter
          org.cabindb.BackupableDBOptions
          org.cabindb.BackupEngine
          org.cabindb.BlockBasedTableConfig
          org.cabindb.BloomFilter
          org.cabindb.CassandraCompactionFilter
          org.cabindb.CassandraValueMergeOperator
          org.cabindb.Checkpoint
          org.cabindb.ClockCache
          org.cabindb.ColumnFamilyHandle
          org.cabindb.ColumnFamilyOptions
          org.cabindb.CompactionJobInfo
          org.cabindb.CompactionJobStats
          org.cabindb.CompactionOptions
          org.cabindb.CompactionOptionsFIFO
          org.cabindb.CompactionOptionsUniversal
          org.cabindb.CompactRangeOptions
          org.cabindb.ComparatorOptions
          org.cabindb.CompressionOptions
          org.cabindb.DBOptions
          org.cabindb.DirectSlice
          org.cabindb.Env
          org.cabindb.EnvOptions
          org.cabindb.Filter
          org.cabindb.FlushOptions
          org.cabindb.HashLinkedListMemTableConfig
          org.cabindb.HashSkipListMemTableConfig
          org.cabindb.HdfsEnv
          org.cabindb.IngestExternalFileOptions
          org.cabindb.Logger
          org.cabindb.LRUCache
          org.cabindb.MemoryUtil
          org.cabindb.MemTableConfig
          org.cabindb.NativeComparatorWrapper
          org.cabindb.NativeLibraryLoader
          org.cabindb.OptimisticTransactionDB
          org.cabindb.OptimisticTransactionOptions
          org.cabindb.Options
          org.cabindb.OptionsUtil
          org.cabindb.PersistentCache
          org.cabindb.PlainTableConfig
          org.cabindb.RateLimiter
          org.cabindb.ReadOptions
          org.cabindb.RemoveEmptyValueCompactionFilter
          org.cabindb.RestoreOptions
          org.cabindb.CabinCallbackObject
          org.cabindb.CabinDB
          org.cabindb.CabinEnv
          org.cabindb.CabinIterator
          org.cabindb.CabinIteratorInterface
          org.cabindb.CabinMemEnv
          org.cabindb.CabinMutableObject
          org.cabindb.CabinObject
          org.cabindb.SkipListMemTableConfig
          org.cabindb.Slice
          org.cabindb.Snapshot
          org.cabindb.SstFileManager
          org.cabindb.SstFileWriter
          org.cabindb.SstFileReader
          org.cabindb.SstFileReaderIterator
          org.cabindb.SstPartitionerFactory
          org.cabindb.SstPartitionerFixedPrefixFactory
          org.cabindb.Statistics
          org.cabindb.StringAppendOperator
          org.cabindb.TableFormatConfig
          org.cabindb.ThreadStatus
          org.cabindb.TimedEnv
          org.cabindb.Transaction
          org.cabindb.TransactionDB
          org.cabindb.TransactionDBOptions
          org.cabindb.TransactionLogIterator
          org.cabindb.TransactionOptions
          org.cabindb.TtlDB
          org.cabindb.UInt64AddOperator
          org.cabindb.VectorMemTableConfig
          org.cabindb.WBWICabinIterator
          org.cabindb.WriteBatch
          org.cabindb.WriteBatch.Handler
          org.cabindb.WriteBatchInterface
          org.cabindb.WriteBatchWithIndex
          org.cabindb.WriteOptions
          org.cabindb.NativeComparatorWrapperTest
          org.cabindb.CabinDBExceptionTest
          org.cabindb.SnapshotTest
          org.cabindb.WriteBatchTest
          org.cabindb.WriteBatchTestInternalHelper
          org.cabindb.WriteBufferManager
  )

  create_javah(
    TARGET cabindbjni_headers
    CLASSES ${NATIVE_JAVA_CLASSES}
    CLASSPATH cabindbjni_classes ${JAVA_TESTCLASSPATH}
    OUTPUT_DIR ${JNI_OUTPUT_DIR}
  )
endif()

if(NOT MSVC)
  set_property(TARGET ${CABINDB_STATIC_LIB} PROPERTY POSITION_INDEPENDENT_CODE ON)
endif()

set(CABINDBJNI_STATIC_LIB cabindbjni${ARTIFACT_SUFFIX})
add_library(${CABINDBJNI_STATIC_LIB} ${JNI_NATIVE_SOURCES})
add_dependencies(${CABINDBJNI_STATIC_LIB} cabindbjni_headers)
target_link_libraries(${CABINDBJNI_STATIC_LIB} ${CABINDB_STATIC_LIB} ${CABINDB_LIB})

if(NOT MINGW)
  set(CABINDBJNI_SHARED_LIB cabindbjni-shared${ARTIFACT_SUFFIX})
  add_library(${CABINDBJNI_SHARED_LIB} SHARED ${JNI_NATIVE_SOURCES})
  add_dependencies(${CABINDBJNI_SHARED_LIB} cabindbjni_headers)
  target_link_libraries(${CABINDBJNI_SHARED_LIB} ${CABINDB_STATIC_LIB} ${CABINDB_LIB})

  set_target_properties(
    ${CABINDBJNI_SHARED_LIB}
    PROPERTIES
    COMPILE_PDB_OUTPUT_DIRECTORY ${CMAKE_CFG_INTDIR}
    COMPILE_PDB_NAME ${CABINDBJNI_STATIC_LIB}.pdb
  )
endif()
