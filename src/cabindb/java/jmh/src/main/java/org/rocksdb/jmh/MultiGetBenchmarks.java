/**
 * Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
 *  This source code is licensed under both the GPLv2 (found in the
 *  COPYING file in the root directory) and Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory).
 */
package org.cabindb.jmh;

import org.openjdk.jmh.annotations.*;
import org.cabindb.*;
import org.cabindb.util.FileUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.cabindb.util.KVUtils.ba;
import static org.cabindb.util.KVUtils.keys;

@State(Scope.Benchmark)
public class MultiGetBenchmarks {

  @Param({
      "no_column_family",
      "1_column_family",
      "20_column_families",
      "100_column_families"
  })
  String columnFamilyTestType;

  @Param("100000")
  int keyCount;

  @Param({
          "10",
          "100",
          "1000",
          "10000",
  })
  int multiGetSize;

  Path dbDir;
  DBOptions options;
  int cfs = 0;  // number of column families
  private AtomicInteger cfHandlesIdx;
  ColumnFamilyHandle[] cfHandles;
  CabinDB db;
  private final AtomicInteger keyIndex = new AtomicInteger();

  @Setup(Level.Trial)
  public void setup() throws IOException, CabinDBException {
    CabinDB.loadLibrary();

    dbDir = Files.createTempDirectory("cabinjava-multiget-benchmarks");

    options = new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);

    final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
    cfDescriptors.add(new ColumnFamilyDescriptor(CabinDB.DEFAULT_COLUMN_FAMILY));

    if ("1_column_family".equals(columnFamilyTestType)) {
      cfs = 1;
    } else if ("20_column_families".equals(columnFamilyTestType)) {
      cfs = 20;
    } else if ("100_column_families".equals(columnFamilyTestType)) {
      cfs = 100;
    }

    if (cfs > 0) {
      cfHandlesIdx = new AtomicInteger(1);
      for (int i = 1; i <= cfs; i++) {
        cfDescriptors.add(new ColumnFamilyDescriptor(ba("cf" + i)));
      }
    }

    final List<ColumnFamilyHandle> cfHandlesList = new ArrayList<>(cfDescriptors.size());
    db = CabinDB.open(options, dbDir.toAbsolutePath().toString(), cfDescriptors, cfHandlesList);
    cfHandles = cfHandlesList.toArray(new ColumnFamilyHandle[0]);

    // store initial data for retrieving via get
    for (int i = 0; i < cfs; i++) {
      for (int j = 0; j < keyCount; j++) {
        db.put(cfHandles[i], ba("key" + j), ba("value" + j));
      }
    }

    try (final FlushOptions flushOptions = new FlushOptions()
            .setWaitForFlush(true)) {
      db.flush(flushOptions);
    }
  }

  @TearDown(Level.Trial)
  public void cleanup() throws IOException {
    for (final ColumnFamilyHandle cfHandle : cfHandles) {
      cfHandle.close();
    }
    db.close();
    options.close();
    FileUtils.delete(dbDir);
  }

  private ColumnFamilyHandle getColumnFamily() {
    if (cfs == 0) {
      return cfHandles[0];
    }  else if (cfs == 1) {
      return cfHandles[1];
    } else {
      int idx = cfHandlesIdx.getAndIncrement();
      if (idx > cfs) {
        cfHandlesIdx.set(1); // doesn't ensure a perfect distribution, but it's ok
        idx = 0;
      }
      return cfHandles[idx];
    }
  }

  /**
   * Reserves the next {@inc} positions in the index.
   *
   * @param inc the number by which to increment the index
   * @param limit the limit for the index
   * @return the index before {@code inc} is added
   */
  private int next(final int inc, final int limit) {
    int idx;
    int nextIdx;
    while (true) {
      idx = keyIndex.get();
      nextIdx = idx + inc;
      if (nextIdx >= limit) {
          nextIdx = inc;
      }

      if (keyIndex.compareAndSet(idx, nextIdx)) {
        break;
      }
    }

    if (nextIdx >= limit) {
      return -1;
    } else {
      return idx;
    }
  }

  @Benchmark
  public List<byte[]> multiGet10() throws CabinDBException {
    final int fromKeyIdx = next(multiGetSize, keyCount);
    final List<byte[]> keys = keys(fromKeyIdx, fromKeyIdx + multiGetSize);
    return db.multiGetAsList(keys);
  }
}
