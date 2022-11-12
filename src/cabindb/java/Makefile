NATIVE_JAVA_CLASSES = \
	org.cabindb.AbstractCompactionFilter\
	org.cabindb.AbstractCompactionFilterFactory\
	org.cabindb.AbstractComparator\
	org.cabindb.AbstractEventListener\
	org.cabindb.AbstractSlice\
	org.cabindb.AbstractTableFilter\
	org.cabindb.AbstractTraceWriter\
	org.cabindb.AbstractTransactionNotifier\
	org.cabindb.AbstractWalFilter\
	org.cabindb.BackupEngine\
	org.cabindb.BackupableDBOptions\
	org.cabindb.BlockBasedTableConfig\
	org.cabindb.BloomFilter\
	org.cabindb.Checkpoint\
	org.cabindb.ClockCache\
	org.cabindb.CassandraCompactionFilter\
	org.cabindb.CassandraValueMergeOperator\
	org.cabindb.ColumnFamilyHandle\
	org.cabindb.ColumnFamilyOptions\
	org.cabindb.CompactionJobInfo\
	org.cabindb.CompactionJobStats\
	org.cabindb.CompactionOptions\
	org.cabindb.CompactionOptionsFIFO\
	org.cabindb.CompactionOptionsUniversal\
	org.cabindb.CompactRangeOptions\
	org.cabindb.ComparatorOptions\
	org.cabindb.CompressionOptions\
	org.cabindb.ConfigOptions\
	org.cabindb.DBOptions\
	org.cabindb.DirectSlice\
	org.cabindb.Env\
	org.cabindb.EnvOptions\
	org.cabindb.FlushOptions\
	org.cabindb.Filter\
	org.cabindb.IngestExternalFileOptions\
	org.cabindb.HashLinkedListMemTableConfig\
	org.cabindb.HashSkipListMemTableConfig\
	org.cabindb.HdfsEnv\
	org.cabindb.ConcurrentTaskLimiter\
	org.cabindb.ConcurrentTaskLimiterImpl\
	org.cabindb.Logger\
	org.cabindb.LRUCache\
	org.cabindb.MemoryUsageType\
	org.cabindb.MemoryUtil\
	org.cabindb.MergeOperator\
	org.cabindb.NativeComparatorWrapper\
	org.cabindb.OptimisticTransactionDB\
	org.cabindb.OptimisticTransactionOptions\
	org.cabindb.Options\
	org.cabindb.OptionsUtil\
	org.cabindb.PersistentCache\
	org.cabindb.PlainTableConfig\
	org.cabindb.RateLimiter\
	org.cabindb.ReadOptions\
	org.cabindb.RemoveEmptyValueCompactionFilter\
	org.cabindb.RestoreOptions\
	org.cabindb.CabinCallbackObject\
	org.cabindb.CabinDB\
	org.cabindb.CabinEnv\
	org.cabindb.CabinIterator\
	org.cabindb.CabinMemEnv\
	org.cabindb.SkipListMemTableConfig\
	org.cabindb.Slice\
	org.cabindb.SstFileManager\
	org.cabindb.SstFileWriter\
	org.cabindb.SstFileReader\
	org.cabindb.SstFileReaderIterator\
	org.cabindb.SstPartitionerFactory\
	org.cabindb.SstPartitionerFixedPrefixFactory\
	org.cabindb.Statistics\
	org.cabindb.ThreadStatus\
	org.cabindb.TimedEnv\
	org.cabindb.Transaction\
	org.cabindb.TransactionDB\
	org.cabindb.TransactionDBOptions\
	org.cabindb.TransactionOptions\
	org.cabindb.TransactionLogIterator\
	org.cabindb.TtlDB\
	org.cabindb.VectorMemTableConfig\
	org.cabindb.Snapshot\
	org.cabindb.StringAppendOperator\
	org.cabindb.UInt64AddOperator\
	org.cabindb.WriteBatch\
	org.cabindb.WriteBatch.Handler\
	org.cabindb.WriteOptions\
	org.cabindb.WriteBatchWithIndex\
	org.cabindb.WriteBufferManager\
	org.cabindb.WBWICabinIterator

NATIVE_JAVA_TEST_CLASSES = \
    org.cabindb.CabinDBExceptionTest\
    org.cabindb.test.TestableEventListener\
    org.cabindb.NativeComparatorWrapperTest.NativeStringComparatorWrapper\
    org.cabindb.WriteBatchTest\
    org.cabindb.WriteBatchTestInternalHelper

CABINDB_MAJOR = $(shell egrep "CABINDB_MAJOR.[0-9]" ../include/cabindb/version.h | cut -d ' ' -f 3)
CABINDB_MINOR = $(shell egrep "CABINDB_MINOR.[0-9]" ../include/cabindb/version.h | cut -d ' ' -f 3)
CABINDB_PATCH = $(shell egrep "CABINDB_PATCH.[0-9]" ../include/cabindb/version.h | cut -d ' ' -f 3)

NATIVE_INCLUDE = ./include
ARCH := $(shell getconf LONG_BIT)
CABINDB_JAR = cabindbjni-$(CABINDB_MAJOR).$(CABINDB_MINOR).$(CABINDB_PATCH)-linux$(ARCH).jar
ifeq ($(PLATFORM), OS_MACOSX)
CABINDB_JAR = cabindbjni-$(CABINDB_MAJOR).$(CABINDB_MINOR).$(CABINDB_PATCH)-osx.jar
endif

JAVA_TESTS = \
	org.cabindb.BackupableDBOptionsTest\
	org.cabindb.BackupEngineTest\
	org.cabindb.BlockBasedTableConfigTest\
	org.cabindb.BuiltinComparatorTest\
	org.cabindb.util.BytewiseComparatorTest\
	org.cabindb.util.BytewiseComparatorIntTest\
	org.cabindb.CheckPointTest\
	org.cabindb.ClockCacheTest\
	org.cabindb.ColumnFamilyOptionsTest\
	org.cabindb.ColumnFamilyTest\
	org.cabindb.CompactionFilterFactoryTest\
	org.cabindb.CompactionJobInfoTest\
	org.cabindb.CompactionJobStatsTest\
	org.cabindb.CompactionOptionsTest\
	org.cabindb.CompactionOptionsFIFOTest\
	org.cabindb.CompactionOptionsUniversalTest\
	org.cabindb.CompactionPriorityTest\
	org.cabindb.CompactionStopStyleTest\
	org.cabindb.ComparatorOptionsTest\
	org.cabindb.CompressionOptionsTest\
	org.cabindb.CompressionTypesTest\
	org.cabindb.DBOptionsTest\
	org.cabindb.DirectSliceTest\
	org.cabindb.util.EnvironmentTest\
	org.cabindb.EnvOptionsTest\
	org.cabindb.EventListenerTest\
	org.cabindb.HdfsEnvTest\
	org.cabindb.IngestExternalFileOptionsTest\
	org.cabindb.util.IntComparatorTest\
	org.cabindb.util.JNIComparatorTest\
	org.cabindb.FilterTest\
	org.cabindb.FlushTest\
	org.cabindb.InfoLogLevelTest\
	org.cabindb.KeyMayExistTest\
	org.cabindb.ConcurrentTaskLimiterTest\
	org.cabindb.LoggerTest\
	org.cabindb.LRUCacheTest\
	org.cabindb.MemoryUtilTest\
	org.cabindb.MemTableTest\
	org.cabindb.MergeTest\
	org.cabindb.MixedOptionsTest\
	org.cabindb.MutableColumnFamilyOptionsTest\
	org.cabindb.MutableDBOptionsTest\
	org.cabindb.NativeComparatorWrapperTest\
	org.cabindb.NativeLibraryLoaderTest\
	org.cabindb.OptimisticTransactionTest\
	org.cabindb.OptimisticTransactionDBTest\
	org.cabindb.OptimisticTransactionOptionsTest\
	org.cabindb.OptionsUtilTest\
	org.cabindb.OptionsTest\
	org.cabindb.PlainTableConfigTest\
	org.cabindb.RateLimiterTest\
	org.cabindb.ReadOnlyTest\
	org.cabindb.ReadOptionsTest\
	org.cabindb.util.ReverseBytewiseComparatorIntTest\
	org.cabindb.CabinDBTest\
	org.cabindb.CabinDBExceptionTest\
	org.cabindb.DefaultEnvTest\
	org.cabindb.CabinIteratorTest\
	org.cabindb.CabinMemEnvTest\
	org.cabindb.util.SizeUnitTest\
	org.cabindb.SecondaryDBTest\
	org.cabindb.SliceTest\
	org.cabindb.SnapshotTest\
	org.cabindb.SstFileManagerTest\
	org.cabindb.SstFileWriterTest\
	org.cabindb.SstFileReaderTest\
	org.cabindb.SstPartitionerTest\
	org.cabindb.TableFilterTest\
	org.cabindb.TimedEnvTest\
	org.cabindb.TransactionTest\
	org.cabindb.TransactionDBTest\
	org.cabindb.TransactionOptionsTest\
	org.cabindb.TransactionDBOptionsTest\
	org.cabindb.TransactionLogIteratorTest\
	org.cabindb.TtlDBTest\
	org.cabindb.StatisticsTest\
	org.cabindb.StatisticsCollectorTest\
	org.cabindb.WalFilterTest\
	org.cabindb.WALRecoveryModeTest\
	org.cabindb.WriteBatchHandlerTest\
	org.cabindb.WriteBatchTest\
	org.cabindb.WriteBatchThreadedTest\
	org.cabindb.WriteOptionsTest\
	org.cabindb.WriteBatchWithIndexTest

MAIN_SRC = src/main/java
TEST_SRC = src/test/java
OUTPUT = target
MAIN_CLASSES = $(OUTPUT)/classes
TEST_CLASSES = $(OUTPUT)/test-classes
JAVADOC = $(OUTPUT)/apidocs

BENCHMARK_MAIN_SRC = benchmark/src/main/java
BENCHMARK_OUTPUT = benchmark/target
BENCHMARK_MAIN_CLASSES = $(BENCHMARK_OUTPUT)/classes

SAMPLES_MAIN_SRC = samples/src/main/java
SAMPLES_OUTPUT = samples/target
SAMPLES_MAIN_CLASSES = $(SAMPLES_OUTPUT)/classes

JAVA_TEST_LIBDIR = test-libs
JAVA_JUNIT_VER = 4.12
JAVA_JUNIT_JAR = junit-$(JAVA_JUNIT_VER).jar
JAVA_JUNIT_JAR_PATH = $(JAVA_TEST_LIBDIR)/$(JAVA_JUNIT_JAR)
JAVA_HAMCREST_VER = 1.3
JAVA_HAMCREST_JAR = hamcrest-core-$(JAVA_HAMCREST_VER).jar
JAVA_HAMCREST_JAR_PATH = $(JAVA_TEST_LIBDIR)/$(JAVA_HAMCREST_JAR)
JAVA_MOCKITO_VER = 1.10.19
JAVA_MOCKITO_JAR = mockito-all-$(JAVA_MOCKITO_VER).jar
JAVA_MOCKITO_JAR_PATH = $(JAVA_TEST_LIBDIR)/$(JAVA_MOCKITO_JAR)
JAVA_CGLIB_VER = 2.2.2
JAVA_CGLIB_JAR = cglib-$(JAVA_CGLIB_VER).jar
JAVA_CGLIB_JAR_PATH = $(JAVA_TEST_LIBDIR)/$(JAVA_CGLIB_JAR)
JAVA_ASSERTJ_VER = 1.7.1
JAVA_ASSERTJ_JAR = assertj-core-$(JAVA_ASSERTJ_VER).jar
JAVA_ASSERTJ_JAR_PATH = $(JAVA_TEST_LIBDIR)/$(JAVA_ASSERTJ_JAR)
JAVA_TESTCLASSPATH = $(JAVA_JUNIT_JAR_PATH):$(JAVA_HAMCREST_JAR_PATH):$(JAVA_MOCKITO_JAR_PATH):$(JAVA_CGLIB_JAR_PATH):$(JAVA_ASSERTJ_JAR_PATH)

MVN_LOCAL = ~/.m2/repository

# Set the default JAVA_ARGS to "" for DEBUG_LEVEL=0
JAVA_ARGS ?=

JAVAC_ARGS ?=

# When debugging add -Xcheck:jni to the java args
ifneq ($(DEBUG_LEVEL),0)
	JAVA_ARGS += -ea -Xcheck:jni
	JAVAC_ARGS += -Xlint:deprecation -Xlint:unchecked
endif

# Using a Facebook AWS account for S3 storage. (maven.org has a history
# of failing in Travis builds.)
DEPS_URL?=https://cabindb-deps.s3-us-west-2.amazonaws.com/jars

clean: clean-not-downloaded clean-downloaded

clean-not-downloaded:
	$(AM_V_at)rm -rf $(NATIVE_INCLUDE)
	$(AM_V_at)rm -rf $(OUTPUT)
	$(AM_V_at)rm -rf $(BENCHMARK_OUTPUT)
	$(AM_V_at)rm -rf $(SAMPLES_OUTPUT)

clean-downloaded:
	$(AM_V_at)rm -rf $(JAVA_TEST_LIBDIR)


javadocs: java
	$(AM_V_GEN)mkdir -p $(JAVADOC)
	$(AM_V_at)javadoc -d $(JAVADOC) -sourcepath $(MAIN_SRC) -subpackages org

javalib: java java_test javadocs

java:
	$(AM_V_GEN)mkdir -p $(MAIN_CLASSES)
ifeq ($(shell java -version 2>&1 | grep 1.7.0 > /dev/null; printf $$?), 0)
	$(AM_V_at)javac $(JAVAC_ARGS) -d $(MAIN_CLASSES)\
		$(MAIN_SRC)/org/cabindb/util/*.java\
		$(MAIN_SRC)/org/cabindb/*.java
else
	$(AM_V_at)javac $(JAVAC_ARGS) -h $(NATIVE_INCLUDE) -d $(MAIN_CLASSES)\
		$(MAIN_SRC)/org/cabindb/util/*.java\
		$(MAIN_SRC)/org/cabindb/*.java
endif
	$(AM_V_at)@cp ../HISTORY.md ./HISTORY-CPP.md
	$(AM_V_at)@rm -f ./HISTORY-CPP.md
ifeq ($(shell java -version 2>&1 | grep 1.7.0 > /dev/null; printf $$?), 0)
	$(AM_V_at)javah -cp $(MAIN_CLASSES) -d $(NATIVE_INCLUDE) -jni $(NATIVE_JAVA_CLASSES)
endif

sample: java
	$(AM_V_GEN)mkdir -p $(SAMPLES_MAIN_CLASSES)
	$(AM_V_at)javac $(JAVAC_ARGS) -cp $(MAIN_CLASSES) -d $(SAMPLES_MAIN_CLASSES) $(SAMPLES_MAIN_SRC)/CabinDBSample.java
	$(AM_V_at)@rm -rf /tmp/cabindbjni
	$(AM_V_at)@rm -rf /tmp/cabindbjni_not_found
	java $(JAVA_ARGS) -Djava.library.path=target -cp $(MAIN_CLASSES):$(SAMPLES_MAIN_CLASSES) CabinDBSample /tmp/cabindbjni
	$(AM_V_at)@rm -rf /tmp/cabindbjni
	$(AM_V_at)@rm -rf /tmp/cabindbjni_not_found

column_family_sample: java
	$(AM_V_GEN)mkdir -p $(SAMPLES_MAIN_CLASSES)
	$(AM_V_at)javac $(JAVAC_ARGS) -cp $(MAIN_CLASSES) -d $(SAMPLES_MAIN_CLASSES) $(SAMPLES_MAIN_SRC)/CabinDBColumnFamilySample.java
	$(AM_V_at)@rm -rf /tmp/cabindbjni
	java $(JAVA_ARGS) -Djava.library.path=target -cp $(MAIN_CLASSES):$(SAMPLES_MAIN_CLASSES) CabinDBColumnFamilySample /tmp/cabindbjni
	$(AM_V_at)@rm -rf /tmp/cabindbjni

transaction_sample: java
	$(AM_V_GEN)mkdir -p $(SAMPLES_MAIN_CLASSES)
	$(AM_V_at)javac -cp $(MAIN_CLASSES) -d $(SAMPLES_MAIN_CLASSES) $(SAMPLES_MAIN_SRC)/TransactionSample.java
	$(AM_V_at)@rm -rf /tmp/cabindbjni
	java -ea -Xcheck:jni -Djava.library.path=target -cp $(MAIN_CLASSES):$(SAMPLES_MAIN_CLASSES) TransactionSample /tmp/cabindbjni
	$(AM_V_at)@rm -rf /tmp/cabindbjni

optimistic_transaction_sample: java
	$(AM_V_GEN)mkdir -p $(SAMPLES_MAIN_CLASSES)
	$(AM_V_at)javac -cp $(MAIN_CLASSES) -d $(SAMPLES_MAIN_CLASSES) $(SAMPLES_MAIN_SRC)/OptimisticTransactionSample.java
	$(AM_V_at)@rm -rf /tmp/cabindbjni
	java -ea -Xcheck:jni -Djava.library.path=target -cp $(MAIN_CLASSES):$(SAMPLES_MAIN_CLASSES) OptimisticTransactionSample /tmp/cabindbjni
	$(AM_V_at)@rm -rf /tmp/cabindbjni

$(JAVA_TEST_LIBDIR):
	mkdir -p "$(JAVA_TEST_LIBDIR)"

$(JAVA_JUNIT_JAR_PATH): $(JAVA_TEST_LIBDIR)
ifneq (,$(wildcard $(MVN_LOCAL)/junit/junit/$(JAVA_JUNIT_VER)/$(JAVA_JUNIT_JAR)))
	cp -v $(MVN_LOCAL)/junit/junit/$(JAVA_JUNIT_VER)/$(JAVA_JUNIT_JAR) $(JAVA_TEST_LIBDIR)
else
	curl --fail --insecure --output $(JAVA_JUNIT_JAR_PATH) --location $(DEPS_URL)/$(JAVA_JUNIT_JAR)
endif

$(JAVA_HAMCREST_JAR_PATH): $(JAVA_TEST_LIBDIR)
ifneq (,$(wildcard $(MVN_LOCAL)/org/hamcrest/hamcrest-core/$(JAVA_HAMCREST_VER)/$(JAVA_HAMCREST_JAR)))
	cp -v $(MVN_LOCAL)/org/hamcrest/hamcrest-core/$(JAVA_HAMCREST_VER)/$(JAVA_HAMCREST_JAR) $(JAVA_TEST_LIBDIR)
else
	curl --fail --insecure --output $(JAVA_HAMCREST_JAR_PATH) --location $(DEPS_URL)/$(JAVA_HAMCREST_JAR)
endif

$(JAVA_MOCKITO_JAR_PATH): $(JAVA_TEST_LIBDIR)
ifneq (,$(wildcard $(MVN_LOCAL)/org/mockito/mockito-all/$(JAVA_MOCKITO_VER)/$(JAVA_MOCKITO_JAR)))
	cp -v $(MVN_LOCAL)/org/mockito/mockito-all/$(JAVA_MOCKITO_VER)/$(JAVA_MOCKITO_JAR) $(JAVA_TEST_LIBDIR)
else
	curl --fail --insecure --output "$(JAVA_MOCKITO_JAR_PATH)" --location $(DEPS_URL)/$(JAVA_MOCKITO_JAR)
endif

$(JAVA_CGLIB_JAR_PATH): $(JAVA_TEST_LIBDIR)
ifneq (,$(wildcard $(MVN_LOCAL)/cglib/cglib/$(JAVA_CGLIB_VER)/$(JAVA_CGLIB_JAR)))
	cp -v $(MVN_LOCAL)/cglib/cglib/$(JAVA_CGLIB_VER)/$(JAVA_CGLIB_JAR) $(JAVA_TEST_LIBDIR)
else
	curl --fail --insecure --output "$(JAVA_CGLIB_JAR_PATH)" --location $(DEPS_URL)/$(JAVA_CGLIB_JAR)
endif

$(JAVA_ASSERTJ_JAR_PATH): $(JAVA_TEST_LIBDIR)
ifneq (,$(wildcard $(MVN_LOCAL)/org/assertj/assertj-core/$(JAVA_ASSERTJ_VER)/$(JAVA_ASSERTJ_JAR)))
	cp -v $(MVN_LOCAL)/org/assertj/assertj-core/$(JAVA_ASSERTJ_VER)/$(JAVA_ASSERTJ_JAR) $(JAVA_TEST_LIBDIR)
else
	curl --fail --insecure --output "$(JAVA_ASSERTJ_JAR_PATH)" --location $(DEPS_URL)/$(JAVA_ASSERTJ_JAR)
endif

resolve_test_deps: $(JAVA_JUNIT_JAR_PATH) $(JAVA_HAMCREST_JAR_PATH) $(JAVA_MOCKITO_JAR_PATH) $(JAVA_CGLIB_JAR_PATH) $(JAVA_ASSERTJ_JAR_PATH)

java_test: java resolve_test_deps
	$(AM_V_GEN)mkdir -p $(TEST_CLASSES)
ifeq ($(shell java -version 2>&1|grep 1.7.0 >/dev/null; printf $$?),0)
	$(AM_V_at)javac $(JAVAC_ARGS) -cp $(MAIN_CLASSES):$(JAVA_TESTCLASSPATH) -d $(TEST_CLASSES)\
		$(TEST_SRC)/org/cabindb/test/*.java\
		$(TEST_SRC)/org/cabindb/util/*.java\
		$(TEST_SRC)/org/cabindb/*.java
	$(AM_V_at)javah -cp $(MAIN_CLASSES):$(TEST_CLASSES) -d $(NATIVE_INCLUDE) -jni $(NATIVE_JAVA_TEST_CLASSES)
else
	$(AM_V_at)javac $(JAVAC_ARGS) -cp $(MAIN_CLASSES):$(JAVA_TESTCLASSPATH) -h $(NATIVE_INCLUDE) -d $(TEST_CLASSES)\
		$(TEST_SRC)/org/cabindb/test/*.java\
		$(TEST_SRC)/org/cabindb/util/*.java\
		$(TEST_SRC)/org/cabindb/*.java
endif

test: java java_test run_test

run_test:
	java $(JAVA_ARGS) -Djava.library.path=target -cp "$(MAIN_CLASSES):$(TEST_CLASSES):$(JAVA_TESTCLASSPATH):target/*" org.cabindb.test.CabinJunitRunner $(JAVA_TESTS)

db_bench: java
	$(AM_V_GEN)mkdir -p $(BENCHMARK_MAIN_CLASSES)
	$(AM_V_at)javac $(JAVAC_ARGS) -cp $(MAIN_CLASSES) -d $(BENCHMARK_MAIN_CLASSES) $(BENCHMARK_MAIN_SRC)/org/cabindb/benchmark/*.java
