// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.cabindb;

import java.nio.ByteBuffer;

/**
 * SstFileWriter is used to create sst files that can be added to the
 * database later. All keys in files generated by SstFileWriter will have
 * sequence number = 0.
 */
public class SstFileWriter extends CabinObject {
  static {
    CabinDB.loadLibrary();
  }

  /**
   * SstFileWriter Constructor.
   *
   * @param envOptions {@link org.cabindb.EnvOptions} instance.
   * @param options {@link org.cabindb.Options} instance.
   * @param comparator the comparator to specify the ordering of keys.
   *
   * @deprecated Use {@link #SstFileWriter(EnvOptions, Options)}.
   * Passing an explicit comparator is deprecated in lieu of passing the
   * comparator as part of options. Use the other constructor instead.
   */
  @Deprecated
  public SstFileWriter(final EnvOptions envOptions, final Options options,
      final AbstractComparator comparator) {
    super(newSstFileWriter(
        envOptions.nativeHandle_, options.nativeHandle_, comparator.nativeHandle_,
        comparator.getComparatorType().getValue()));
  }

  /**
   * SstFileWriter Constructor.
   *
   * @param envOptions {@link org.cabindb.EnvOptions} instance.
   * @param options {@link org.cabindb.Options} instance.
   */
  public SstFileWriter(final EnvOptions envOptions, final Options options) {
    super(newSstFileWriter(
        envOptions.nativeHandle_, options.nativeHandle_));
  }

  /**
   * Prepare SstFileWriter to write to a file.
   *
   * @param filePath the location of file
   *
   * @throws CabinDBException thrown if error happens in underlying
   *    native library.
   */
  public void open(final String filePath) throws CabinDBException {
    open(nativeHandle_, filePath);
  }

  /**
   * Add a Put key with value to currently opened file.
   *
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   *
   * @throws CabinDBException thrown if error happens in underlying
   *    native library.
   *
   * @deprecated Use {@link #put(Slice, Slice)}
   */
  @Deprecated
  public void add(final Slice key, final Slice value)
      throws CabinDBException {
    put(nativeHandle_, key.getNativeHandle(), value.getNativeHandle());
  }

  /**
   * Add a Put key with value to currently opened file.
   *
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   *
   * @throws CabinDBException thrown if error happens in underlying
   *    native library.
   *
   * @deprecated Use {@link #put(DirectSlice, DirectSlice)}
   */
  @Deprecated
  public void add(final DirectSlice key, final DirectSlice value)
      throws CabinDBException {
    put(nativeHandle_, key.getNativeHandle(), value.getNativeHandle());
  }

  /**
   * Add a Put key with value to currently opened file.
   *
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   *
   * @throws CabinDBException thrown if error happens in underlying
   *    native library.
   */
  public void put(final Slice key, final Slice value) throws CabinDBException {
    put(nativeHandle_, key.getNativeHandle(), value.getNativeHandle());
  }

  /**
   * Add a Put key with value to currently opened file.
   *
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   *
   * @throws CabinDBException thrown if error happens in underlying
   *    native library.
   */
  public void put(final DirectSlice key, final DirectSlice value)
      throws CabinDBException {
    put(nativeHandle_, key.getNativeHandle(), value.getNativeHandle());
  }

  /**
   * Add a Put key with value to currently opened file.
   *
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   *
   * @throws CabinDBException thrown if error happens in underlying
   *    native library.
   */
  public void put(final ByteBuffer key, final ByteBuffer value) throws CabinDBException {
    assert key.isDirect() && value.isDirect();
    putDirect(nativeHandle_, key, key.position(), key.remaining(), value, value.position(),
        value.remaining());
    key.position(key.limit());
    value.position(value.limit());
  }

 /**
   * Add a Put key with value to currently opened file.
   *
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   *
   * @throws CabinDBException thrown if error happens in underlying
   *    native library.
   */
  public void put(final byte[] key, final byte[] value) throws CabinDBException {
    put(nativeHandle_, key, value);
  }

  /**
   * Add a Merge key with value to currently opened file.
   *
   * @param key the specified key to be merged.
   * @param value the value to be merged with the current value for
   * the specified key.
   *
   * @throws CabinDBException thrown if error happens in underlying
   *    native library.
   */
  public void merge(final Slice key, final Slice value)
      throws CabinDBException {
    merge(nativeHandle_, key.getNativeHandle(), value.getNativeHandle());
  }

  /**
   * Add a Merge key with value to currently opened file.
   *
   * @param key the specified key to be merged.
   * @param value the value to be merged with the current value for
   * the specified key.
   *
   * @throws CabinDBException thrown if error happens in underlying
   *    native library.
   */
  public void merge(final byte[] key, final byte[] value)
      throws CabinDBException {
    merge(nativeHandle_, key, value);
  }

  /**
   * Add a Merge key with value to currently opened file.
   *
   * @param key the specified key to be merged.
   * @param value the value to be merged with the current value for
   * the specified key.
   *
   * @throws CabinDBException thrown if error happens in underlying
   *    native library.
   */
  public void merge(final DirectSlice key, final DirectSlice value)
      throws CabinDBException {
    merge(nativeHandle_, key.getNativeHandle(), value.getNativeHandle());
  }

  /**
   * Add a deletion key to currently opened file.
   *
   * @param key the specified key to be deleted.
   *
   * @throws CabinDBException thrown if error happens in underlying
   *    native library.
   */
  public void delete(final Slice key) throws CabinDBException {
    delete(nativeHandle_, key.getNativeHandle());
  }

  /**
   * Add a deletion key to currently opened file.
   *
   * @param key the specified key to be deleted.
   *
   * @throws CabinDBException thrown if error happens in underlying
   *    native library.
   */
  public void delete(final DirectSlice key) throws CabinDBException {
    delete(nativeHandle_, key.getNativeHandle());
  }

  /**
   * Add a deletion key to currently opened file.
   *
   * @param key the specified key to be deleted.
   *
   * @throws CabinDBException thrown if error happens in underlying
   *    native library.
   */
  public void delete(final byte[] key) throws CabinDBException {
    delete(nativeHandle_, key);
  }

  /**
   * Finish the process and close the sst file.
   *
   * @throws CabinDBException thrown if error happens in underlying
   *    native library.
   */
  public void finish() throws CabinDBException {
    finish(nativeHandle_);
  }

  /**
   * Return the current file size.
   *
   * @throws CabinDBException thrown if error happens in underlying
   *    native library.
   */
  public long fileSize() throws CabinDBException {
    return fileSize(nativeHandle_);
  }

  private native static long newSstFileWriter(
      final long envOptionsHandle, final long optionsHandle,
      final long userComparatorHandle, final byte comparatorType);

  private native static long newSstFileWriter(final long envOptionsHandle,
      final long optionsHandle);

  private native void open(final long handle, final String filePath)
      throws CabinDBException;

  private native void put(final long handle, final long keyHandle,
      final long valueHandle) throws CabinDBException;
      
  private native void put(final long handle, final byte[] key,
      final byte[] value) throws CabinDBException;

  private native void putDirect(long handle, ByteBuffer key, int keyOffset, int keyLength,
      ByteBuffer value, int valueOffset, int valueLength) throws CabinDBException;

  private native long fileSize(long handle) throws CabinDBException;

  private native void merge(final long handle, final long keyHandle,
      final long valueHandle) throws CabinDBException;

  private native void merge(final long handle, final byte[] key,
      final byte[] value) throws CabinDBException;

  private native void delete(final long handle, final long keyHandle)
      throws CabinDBException;

  private native void delete(final long handle, final byte[] key)
      throws CabinDBException;

  private native void finish(final long handle) throws CabinDBException;

  @Override protected final native void disposeInternal(final long handle);
}
