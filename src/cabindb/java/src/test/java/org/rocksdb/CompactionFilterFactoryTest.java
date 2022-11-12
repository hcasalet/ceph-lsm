// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.cabindb;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.cabindb.test.RemoveEmptyValueCompactionFilterFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class CompactionFilterFactoryTest {

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void columnFamilyOptions_setCompactionFilterFactory()
      throws CabinDBException {
    try(final DBOptions options = new DBOptions()
            .setCreateIfMissing(true)
            .setCreateMissingColumnFamilies(true);
        final RemoveEmptyValueCompactionFilterFactory compactionFilterFactory
            = new RemoveEmptyValueCompactionFilterFactory();
        final ColumnFamilyOptions new_cf_opts
            = new ColumnFamilyOptions()
            .setCompactionFilterFactory(compactionFilterFactory)) {

      final List<ColumnFamilyDescriptor> cfNames = Arrays.asList(
          new ColumnFamilyDescriptor(CabinDB.DEFAULT_COLUMN_FAMILY),
          new ColumnFamilyDescriptor("new_cf".getBytes(), new_cf_opts));

      final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

      try (final CabinDB cabinDb =
               CabinDB.open(options, dbFolder.getRoot().getAbsolutePath(), cfNames, cfHandles)) {
        final byte[] key1 = "key1".getBytes();
        final byte[] key2 = "key2".getBytes();

        final byte[] value1 = "value1".getBytes();
        final byte[] value2 = new byte[0];

        cabinDb.put(cfHandles.get(1), key1, value1);
        cabinDb.put(cfHandles.get(1), key2, value2);

        cabinDb.compactRange(cfHandles.get(1));

        assertThat(cabinDb.get(cfHandles.get(1), key1)).isEqualTo(value1);
        final boolean exists = cabinDb.keyMayExist(cfHandles.get(1), key2, null);
        assertThat(exists).isFalse();
      }
    }
  }
}
