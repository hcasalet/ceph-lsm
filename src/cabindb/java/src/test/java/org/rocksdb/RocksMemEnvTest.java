// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.cabindb;

import org.junit.ClassRule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CabinMemEnvTest {

  @ClassRule
  public static final CabinNativeLibraryResource CABIN_NATIVE_LIBRARY_RESOURCE =
      new CabinNativeLibraryResource();

  @Test
  public void memEnvFillAndReopen() throws CabinDBException {

    final byte[][] keys = {
        "aaa".getBytes(),
        "bbb".getBytes(),
        "ccc".getBytes()
    };

    final byte[][] values = {
        "foo".getBytes(),
        "bar".getBytes(),
        "baz".getBytes()
    };

    try (final Env env = new CabinMemEnv(Env.getDefault());
         final Options options = new Options()
             .setCreateIfMissing(true)
             .setEnv(env);
         final FlushOptions flushOptions = new FlushOptions()
             .setWaitForFlush(true);
    ) {
      try (final CabinDB db = CabinDB.open(options, "dir/db")) {
        // write key/value pairs using MemEnv
        for (int i = 0; i < keys.length; i++) {
          db.put(keys[i], values[i]);
        }

        // read key/value pairs using MemEnv
        for (int i = 0; i < keys.length; i++) {
          assertThat(db.get(keys[i])).isEqualTo(values[i]);
        }

        // Check iterator access
        try (final CabinIterator iterator = db.newIterator()) {
          iterator.seekToFirst();
          for (int i = 0; i < keys.length; i++) {
            assertThat(iterator.isValid()).isTrue();
            assertThat(iterator.key()).isEqualTo(keys[i]);
            assertThat(iterator.value()).isEqualTo(values[i]);
            iterator.next();
          }
          // reached end of database
          assertThat(iterator.isValid()).isFalse();
        }

        // flush
        db.flush(flushOptions);

        // read key/value pairs after flush using MemEnv
        for (int i = 0; i < keys.length; i++) {
          assertThat(db.get(keys[i])).isEqualTo(values[i]);
        }
      }

      options.setCreateIfMissing(false);

      // After reopen the values shall still be in the mem env.
      // as long as the env is not freed.
      try (final CabinDB db = CabinDB.open(options, "dir/db")) {
        // read key/value pairs using MemEnv
        for (int i = 0; i < keys.length; i++) {
          assertThat(db.get(keys[i])).isEqualTo(values[i]);
        }
      }
    }
  }

  @Test
  public void multipleDatabaseInstances() throws CabinDBException {
    // db - keys
    final byte[][] keys = {
        "aaa".getBytes(),
        "bbb".getBytes(),
        "ccc".getBytes()
    };
    // otherDb - keys
    final byte[][] otherKeys = {
        "111".getBytes(),
        "222".getBytes(),
        "333".getBytes()
    };
    // values
    final byte[][] values = {
        "foo".getBytes(),
        "bar".getBytes(),
        "baz".getBytes()
    };

    try (final Env env = new CabinMemEnv(Env.getDefault());
         final Options options = new Options()
             .setCreateIfMissing(true)
             .setEnv(env);
         final CabinDB db = CabinDB.open(options, "dir/db");
         final CabinDB otherDb = CabinDB.open(options, "dir/otherDb")
    ) {
      // write key/value pairs using MemEnv
      // to db and to otherDb.
      for (int i = 0; i < keys.length; i++) {
        db.put(keys[i], values[i]);
        otherDb.put(otherKeys[i], values[i]);
      }

      // verify key/value pairs after flush using MemEnv
      for (int i = 0; i < keys.length; i++) {
        // verify db
        assertThat(db.get(otherKeys[i])).isNull();
        assertThat(db.get(keys[i])).isEqualTo(values[i]);

        // verify otherDb
        assertThat(otherDb.get(keys[i])).isNull();
        assertThat(otherDb.get(otherKeys[i])).isEqualTo(values[i]);
      }
    }
  }

  @Test(expected = CabinDBException.class)
  public void createIfMissingFalse() throws CabinDBException {
    try (final Env env = new CabinMemEnv(Env.getDefault());
         final Options options = new Options()
             .setCreateIfMissing(false)
             .setEnv(env);
         final CabinDB db = CabinDB.open(options, "db/dir")) {
      // shall throw an exception because db dir does not
      // exist.
    }
  }
}
