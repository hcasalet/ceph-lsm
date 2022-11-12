// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.cabindb;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CompactionPriorityTest {

  @Test(expected = IllegalArgumentException.class)
  public void failIfIllegalByteValueProvided() {
    CompactionPriority.getCompactionPriority((byte) -1);
  }

  @Test
  public void getCompactionPriority() {
    assertThat(CompactionPriority.getCompactionPriority(
        CompactionPriority.OldestLargestSeqFirst.getValue()))
            .isEqualTo(CompactionPriority.OldestLargestSeqFirst);
  }

  @Test
  public void valueOf() {
    assertThat(CompactionPriority.valueOf("OldestSmallestSeqFirst")).
        isEqualTo(CompactionPriority.OldestSmallestSeqFirst);
  }
}
