// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.cabindb;

import java.nio.ByteBuffer;

public abstract class AbstractWriteBatch extends CabinObject
    implements WriteBatchInterface {

  protected AbstractWriteBatch(final long nativeHandle) {
    super(nativeHandle);
  }

  @Override
  public int count() {
    return count0(nativeHandle_);
  }

  @Override
  public void put(byte[] key, byte[] value) throws CabinDBException {
    put(nativeHandle_, key, key.length, value, value.length);
  }

  @Override
  public void put(ColumnFamilyHandle columnFamilyHandle, byte[] key,
      byte[] value) throws CabinDBException {
    put(nativeHandle_, key, key.length, value, value.length,
        columnFamilyHandle.nativeHandle_);
  }

  @Override
  public void merge(byte[] key, byte[] value) throws CabinDBException {
    merge(nativeHandle_, key, key.length, value, value.length);
  }

  @Override
  public void merge(ColumnFamilyHandle columnFamilyHandle, byte[] key,
      byte[] value) throws CabinDBException {
    merge(nativeHandle_, key, key.length, value, value.length,
        columnFamilyHandle.nativeHandle_);
  }

  @Override
  @Deprecated
  public void remove(byte[] key) throws CabinDBException {
    delete(nativeHandle_, key, key.length);
  }

  @Override
  @Deprecated
  public void remove(ColumnFamilyHandle columnFamilyHandle, byte[] key)
      throws CabinDBException {
    delete(nativeHandle_, key, key.length, columnFamilyHandle.nativeHandle_);
  }

  public void put(ByteBuffer key, ByteBuffer value) throws CabinDBException {
    assert key.isDirect() && value.isDirect();
    putDirect(nativeHandle_, key, key.position(), key.remaining(), value, value.position(),
        value.remaining(), 0);
    key.position(key.limit());
    value.position(value.limit());
  }

  @Override
  public void put(ColumnFamilyHandle columnFamilyHandle, ByteBuffer key, ByteBuffer value)
      throws CabinDBException {
    assert key.isDirect() && value.isDirect();
    putDirect(nativeHandle_, key, key.position(), key.remaining(), value, value.position(),
        value.remaining(), columnFamilyHandle.nativeHandle_);
    key.position(key.limit());
    value.position(value.limit());
  }

  @Override
  public void delete(byte[] key) throws CabinDBException {
    delete(nativeHandle_, key, key.length);
  }

  @Override
  public void delete(ColumnFamilyHandle columnFamilyHandle, byte[] key)
      throws CabinDBException {
    delete(nativeHandle_, key, key.length, columnFamilyHandle.nativeHandle_);
  }


  @Override
  public void singleDelete(byte[] key) throws CabinDBException {
    singleDelete(nativeHandle_, key, key.length);
  }

  @Override
  public void singleDelete(ColumnFamilyHandle columnFamilyHandle, byte[] key)
      throws CabinDBException {
    singleDelete(nativeHandle_, key, key.length, columnFamilyHandle.nativeHandle_);
  }

  @Override
  public void deleteRange(byte[] beginKey, byte[] endKey)
      throws CabinDBException {
    deleteRange(nativeHandle_, beginKey, beginKey.length, endKey, endKey.length);
  }

  @Override
  public void deleteRange(ColumnFamilyHandle columnFamilyHandle,
      byte[] beginKey, byte[] endKey) throws CabinDBException {
    deleteRange(nativeHandle_, beginKey, beginKey.length, endKey, endKey.length,
        columnFamilyHandle.nativeHandle_);
  }

  public void remove(ByteBuffer key) throws CabinDBException {
    removeDirect(nativeHandle_, key, key.position(), key.remaining(), 0);
    key.position(key.limit());
  }

  @Override
  public void remove(ColumnFamilyHandle columnFamilyHandle, ByteBuffer key)
      throws CabinDBException {
    removeDirect(
        nativeHandle_, key, key.position(), key.remaining(), columnFamilyHandle.nativeHandle_);
    key.position(key.limit());
  }

  @Override
  public void putLogData(byte[] blob) throws CabinDBException {
    putLogData(nativeHandle_, blob, blob.length);
  }

  @Override
  public void clear() {
    clear0(nativeHandle_);
  }

  @Override
  public void setSavePoint() {
    setSavePoint0(nativeHandle_);
  }

  @Override
  public void rollbackToSavePoint() throws CabinDBException {
    rollbackToSavePoint0(nativeHandle_);
  }

  @Override
  public void popSavePoint() throws CabinDBException {
    popSavePoint(nativeHandle_);
  }

  @Override
  public void setMaxBytes(final long maxBytes) {
    setMaxBytes(nativeHandle_, maxBytes);
  }

  @Override
  public WriteBatch getWriteBatch() {
    return getWriteBatch(nativeHandle_);
  }

  abstract int count0(final long handle);

  abstract void put(final long handle, final byte[] key, final int keyLen,
      final byte[] value, final int valueLen) throws CabinDBException;

  abstract void put(final long handle, final byte[] key, final int keyLen,
      final byte[] value, final int valueLen, final long cfHandle)
      throws CabinDBException;

  abstract void putDirect(final long handle, final ByteBuffer key, final int keyOffset,
      final int keyLength, final ByteBuffer value, final int valueOffset, final int valueLength,
      final long cfHandle) throws CabinDBException;

  abstract void merge(final long handle, final byte[] key, final int keyLen,
      final byte[] value, final int valueLen) throws CabinDBException;

  abstract void merge(final long handle, final byte[] key, final int keyLen,
      final byte[] value, final int valueLen, final long cfHandle)
      throws CabinDBException;

  abstract void delete(final long handle, final byte[] key,
      final int keyLen) throws CabinDBException;

  abstract void delete(final long handle, final byte[] key,
      final int keyLen, final long cfHandle) throws CabinDBException;

  abstract void singleDelete(final long handle, final byte[] key,
                       final int keyLen) throws CabinDBException;

  abstract void singleDelete(final long handle, final byte[] key,
                       final int keyLen, final long cfHandle) throws CabinDBException;

  abstract void removeDirect(final long handle, final ByteBuffer key, final int keyOffset,
      final int keyLength, final long cfHandle) throws CabinDBException;

  abstract void deleteRange(final long handle, final byte[] beginKey, final int beginKeyLen,
      final byte[] endKey, final int endKeyLen) throws CabinDBException;

  abstract void deleteRange(final long handle, final byte[] beginKey, final int beginKeyLen,
      final byte[] endKey, final int endKeyLen, final long cfHandle) throws CabinDBException;

  abstract void putLogData(final long handle, final byte[] blob,
      final int blobLen) throws CabinDBException;

  abstract void clear0(final long handle);

  abstract void setSavePoint0(final long handle);

  abstract void rollbackToSavePoint0(final long handle);

  abstract void popSavePoint(final long handle) throws CabinDBException;

  abstract void setMaxBytes(final long handle, long maxBytes);

  abstract WriteBatch getWriteBatch(final long handle);
}
