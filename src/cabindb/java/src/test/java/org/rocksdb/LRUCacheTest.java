// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.cabindb;

import org.junit.Test;

public class LRUCacheTest {

  static {
    CabinDB.loadLibrary();
  }

  @Test
  public void newLRUCache() {
    final long capacity = 1000;
    final int numShardBits = 16;
    final boolean strictCapacityLimit = true;
    final double highPriPoolRatio = 5;
    try(final Cache lruCache = new LRUCache(capacity,
        numShardBits, strictCapacityLimit, highPriPoolRatio)) {
      //no op
    }
  }
}
