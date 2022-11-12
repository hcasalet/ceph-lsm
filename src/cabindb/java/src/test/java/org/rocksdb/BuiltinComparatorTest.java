// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.cabindb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.assertj.core.api.Assertions.assertThat;

public class BuiltinComparatorTest {

  @ClassRule
  public static final CabinNativeLibraryResource CABIN_NATIVE_LIBRARY_RESOURCE =
      new CabinNativeLibraryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void builtinForwardComparator()
      throws CabinDBException {
    try (final Options options = new Options()
        .setCreateIfMissing(true)
        .setComparator(BuiltinComparator.BYTEWISE_COMPARATOR);
         final CabinDB cabinDb = CabinDB.open(options,
             dbFolder.getRoot().getAbsolutePath())
    ) {
      cabinDb.put("abc1".getBytes(), "abc1".getBytes());
      cabinDb.put("abc2".getBytes(), "abc2".getBytes());
      cabinDb.put("abc3".getBytes(), "abc3".getBytes());

      try(final CabinIterator cabinIterator = cabinDb.newIterator()) {
        // Iterate over keys using a iterator
        cabinIterator.seekToFirst();
        assertThat(cabinIterator.isValid()).isTrue();
        assertThat(cabinIterator.key()).isEqualTo(
            "abc1".getBytes());
        assertThat(cabinIterator.value()).isEqualTo(
            "abc1".getBytes());
        cabinIterator.next();
        assertThat(cabinIterator.isValid()).isTrue();
        assertThat(cabinIterator.key()).isEqualTo(
            "abc2".getBytes());
        assertThat(cabinIterator.value()).isEqualTo(
            "abc2".getBytes());
        cabinIterator.next();
        assertThat(cabinIterator.isValid()).isTrue();
        assertThat(cabinIterator.key()).isEqualTo(
            "abc3".getBytes());
        assertThat(cabinIterator.value()).isEqualTo(
            "abc3".getBytes());
        cabinIterator.next();
        assertThat(cabinIterator.isValid()).isFalse();
        // Get last one
        cabinIterator.seekToLast();
        assertThat(cabinIterator.isValid()).isTrue();
        assertThat(cabinIterator.key()).isEqualTo(
            "abc3".getBytes());
        assertThat(cabinIterator.value()).isEqualTo(
            "abc3".getBytes());
        // Seek for abc
        cabinIterator.seek("abc".getBytes());
        assertThat(cabinIterator.isValid()).isTrue();
        assertThat(cabinIterator.key()).isEqualTo(
            "abc1".getBytes());
        assertThat(cabinIterator.value()).isEqualTo(
            "abc1".getBytes());
      }
    }
  }

  @Test
  public void builtinReverseComparator()
      throws CabinDBException {
    try (final Options options = new Options()
        .setCreateIfMissing(true)
        .setComparator(BuiltinComparator.REVERSE_BYTEWISE_COMPARATOR);
         final CabinDB cabinDb = CabinDB.open(options,
             dbFolder.getRoot().getAbsolutePath())
    ) {

      cabinDb.put("abc1".getBytes(), "abc1".getBytes());
      cabinDb.put("abc2".getBytes(), "abc2".getBytes());
      cabinDb.put("abc3".getBytes(), "abc3".getBytes());

      try (final CabinIterator cabinIterator = cabinDb.newIterator()) {
        // Iterate over keys using a iterator
        cabinIterator.seekToFirst();
        assertThat(cabinIterator.isValid()).isTrue();
        assertThat(cabinIterator.key()).isEqualTo(
            "abc3".getBytes());
        assertThat(cabinIterator.value()).isEqualTo(
            "abc3".getBytes());
        cabinIterator.next();
        assertThat(cabinIterator.isValid()).isTrue();
        assertThat(cabinIterator.key()).isEqualTo(
            "abc2".getBytes());
        assertThat(cabinIterator.value()).isEqualTo(
            "abc2".getBytes());
        cabinIterator.next();
        assertThat(cabinIterator.isValid()).isTrue();
        assertThat(cabinIterator.key()).isEqualTo(
            "abc1".getBytes());
        assertThat(cabinIterator.value()).isEqualTo(
            "abc1".getBytes());
        cabinIterator.next();
        assertThat(cabinIterator.isValid()).isFalse();
        // Get last one
        cabinIterator.seekToLast();
        assertThat(cabinIterator.isValid()).isTrue();
        assertThat(cabinIterator.key()).isEqualTo(
            "abc1".getBytes());
        assertThat(cabinIterator.value()).isEqualTo(
            "abc1".getBytes());
        // Will be invalid because abc is after abc1
        cabinIterator.seek("abc".getBytes());
        assertThat(cabinIterator.isValid()).isFalse();
        // Will be abc3 because the next one after abc999
        // is abc3
        cabinIterator.seek("abc999".getBytes());
        assertThat(cabinIterator.key()).isEqualTo(
            "abc3".getBytes());
        assertThat(cabinIterator.value()).isEqualTo(
            "abc3".getBytes());
      }
    }
  }

  @Test
  public void builtinComparatorEnum(){
    assertThat(BuiltinComparator.BYTEWISE_COMPARATOR.ordinal())
        .isEqualTo(0);
    assertThat(
        BuiltinComparator.REVERSE_BYTEWISE_COMPARATOR.ordinal())
        .isEqualTo(1);
    assertThat(BuiltinComparator.values().length).isEqualTo(2);
    assertThat(BuiltinComparator.valueOf("BYTEWISE_COMPARATOR")).
        isEqualTo(BuiltinComparator.BYTEWISE_COMPARATOR);
  }
}
