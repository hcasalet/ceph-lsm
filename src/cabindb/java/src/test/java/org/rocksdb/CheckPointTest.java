// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
package org.cabindb;


import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.assertj.core.api.Assertions.assertThat;

public class CheckPointTest {

  @ClassRule
  public static final CabinNativeLibraryResource CABIN_NATIVE_LIBRARY_RESOURCE =
      new CabinNativeLibraryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Rule
  public TemporaryFolder checkpointFolder = new TemporaryFolder();

  @Test
  public void checkPoint() throws CabinDBException {
    try (final Options options = new Options().
        setCreateIfMissing(true)) {

      try (final CabinDB db = CabinDB.open(options,
          dbFolder.getRoot().getAbsolutePath())) {
        db.put("key".getBytes(), "value".getBytes());
        try (final Checkpoint checkpoint = Checkpoint.create(db)) {
          checkpoint.createCheckpoint(checkpointFolder.
              getRoot().getAbsolutePath() + "/snapshot1");
          db.put("key2".getBytes(), "value2".getBytes());
          checkpoint.createCheckpoint(checkpointFolder.
              getRoot().getAbsolutePath() + "/snapshot2");
        }
      }

      try (final CabinDB db = CabinDB.open(options,
          checkpointFolder.getRoot().getAbsolutePath() +
              "/snapshot1")) {
        assertThat(new String(db.get("key".getBytes()))).
            isEqualTo("value");
        assertThat(db.get("key2".getBytes())).isNull();
      }

      try (final CabinDB db = CabinDB.open(options,
          checkpointFolder.getRoot().getAbsolutePath() +
              "/snapshot2")) {
        assertThat(new String(db.get("key".getBytes()))).
            isEqualTo("value");
        assertThat(new String(db.get("key2".getBytes()))).
            isEqualTo("value2");
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void failIfDbIsNull() {
    try (final Checkpoint checkpoint = Checkpoint.create(null)) {

    }
  }

  @Test(expected = IllegalStateException.class)
  public void failIfDbNotInitialized() throws CabinDBException {
    try (final CabinDB db = CabinDB.open(
        dbFolder.getRoot().getAbsolutePath())) {
      db.close();
      Checkpoint.create(db);
    }
  }

  @Test(expected = CabinDBException.class)
  public void failWithIllegalPath() throws CabinDBException {
    try (final CabinDB db = CabinDB.open(dbFolder.getRoot().getAbsolutePath());
         final Checkpoint checkpoint = Checkpoint.create(db)) {
      checkpoint.createCheckpoint("/Z:///:\\C:\\TZ/-");
    }
  }
}
