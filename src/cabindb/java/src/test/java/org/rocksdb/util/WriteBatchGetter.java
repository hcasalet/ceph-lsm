// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
package org.cabindb.util;

import org.cabindb.CabinDBException;
import org.cabindb.WriteBatch;

import java.util.Arrays;

public class WriteBatchGetter extends WriteBatch.Handler {

  private int columnFamilyId = -1;
  private final byte[] key;
  private byte[] value;

  public WriteBatchGetter(final byte[] key) {
    this.key = key;
  }

  public byte[] getValue() {
    return value;
  }

  @Override
  public void put(final int columnFamilyId, final byte[] key,
                  final byte[] value) {
    if(Arrays.equals(this.key, key)) {
      this.columnFamilyId = columnFamilyId;
      this.value = value;
    }
  }

  @Override
  public void put(final byte[] key, final byte[] value) {
    if(Arrays.equals(this.key, key)) {
      this.value = value;
    }
  }

  @Override
  public void merge(final int columnFamilyId, final byte[] key,
                    final byte[] value) {
    if(Arrays.equals(this.key, key)) {
      this.columnFamilyId = columnFamilyId;
      this.value = value;
    }
  }

  @Override
  public void merge(final byte[] key, final byte[] value) {
    if(Arrays.equals(this.key, key)) {
      this.value = value;
    }
  }

  @Override
  public void delete(final int columnFamilyId, final byte[] key) {
    if(Arrays.equals(this.key, key)) {
      this.columnFamilyId = columnFamilyId;
      this.value = null;
    }
  }

  @Override
  public void delete(final byte[] key) {
    if(Arrays.equals(this.key, key)) {
      this.value = null;
    }
  }

  @Override
  public void singleDelete(final int columnFamilyId, final byte[] key) {
    if(Arrays.equals(this.key, key)) {
      this.columnFamilyId = columnFamilyId;
      this.value = null;
    }
  }

  @Override
  public void singleDelete(final byte[] key) {
    if(Arrays.equals(this.key, key)) {
      this.value = null;
    }
  }

  @Override
  public void deleteRange(final int columnFamilyId, final byte[] beginKey,
                          final byte[] endKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteRange(final byte[] beginKey, final byte[] endKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void logData(final byte[] blob) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putBlobIndex(final int columnFamilyId, final byte[] key,
                           final byte[] value) {
    if(Arrays.equals(this.key, key)) {
      this.columnFamilyId = columnFamilyId;
      this.value = value;
    }
  }

  @Override
  public void markBeginPrepare() throws CabinDBException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void markEndPrepare(final byte[] xid) throws CabinDBException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void markNoop(final boolean emptyBatch) throws CabinDBException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void markRollback(final byte[] xid) throws CabinDBException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void markCommit(final byte[] xid) throws CabinDBException {
    throw new UnsupportedOperationException();
  }
}
