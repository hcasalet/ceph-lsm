// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.cabindb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static java.nio.charset.StandardCharsets.UTF_8;

public class HdfsEnvTest {

  @ClassRule
  public static final CabinNativeLibraryResource CABIN_NATIVE_LIBRARY_RESOURCE =
      new CabinNativeLibraryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  // expect org.cabindb.CabinDBException: Not compiled with hdfs support
  @Test(expected = CabinDBException.class)
  public void construct() throws CabinDBException {
    try (final Env env = new HdfsEnv("hdfs://localhost:5000")) {
      // no-op
    }
  }

  // expect org.cabindb.CabinDBException: Not compiled with hdfs support
  @Test(expected = CabinDBException.class)
  public void construct_integration() throws CabinDBException {
    try (final Env env = new HdfsEnv("hdfs://localhost:5000");
         final Options options = new Options()
             .setCreateIfMissing(true)
             .setEnv(env);
    ) {
      try (final CabinDB db = CabinDB.open(options, dbFolder.getRoot().getPath())) {
        db.put("key1".getBytes(UTF_8), "value1".getBytes(UTF_8));
      }
    }
  }
}
