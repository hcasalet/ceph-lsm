// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.cabindb;

import java.nio.ByteBuffer;

/**
 * Base class implementation for Cabin Iterators
 * in the Java API
 *
 * <p>Multiple threads can invoke const methods on an CabinIterator without
 * external synchronization, but if any of the threads may call a
 * non-const method, all threads accessing the same CabinIterator must use
 * external synchronization.</p>
 *
 * @param <P> The type of the Parent Object from which the Cabin Iterator was
 *          created. This is used by disposeInternal to avoid double-free
 *          issues with the underlying C++ object.
 * @see org.cabindb.CabinObject
 */
public abstract class AbstractCabinIterator<P extends CabinObject>
    extends CabinObject implements CabinIteratorInterface {
  final P parent_;

  protected AbstractCabinIterator(final P parent,
      final long nativeHandle) {
    super(nativeHandle);
    // parent must point to a valid CabinDB instance.
    assert (parent != null);
    // CabinIterator must hold a reference to the related parent instance
    // to guarantee that while a GC cycle starts CabinIterator instances
    // are freed prior to parent instances.
    parent_ = parent;
  }

  @Override
  public boolean isValid() {
    assert (isOwningHandle());
    return isValid0(nativeHandle_);
  }

  @Override
  public void seekToFirst() {
    assert (isOwningHandle());
    seekToFirst0(nativeHandle_);
  }

  @Override
  public void seekToLast() {
    assert (isOwningHandle());
    seekToLast0(nativeHandle_);
  }

  @Override
  public void seek(byte[] target) {
    assert (isOwningHandle());
    seek0(nativeHandle_, target, target.length);
  }

 @Override
 public void seekForPrev(byte[] target) {
   assert (isOwningHandle());
   seekForPrev0(nativeHandle_, target, target.length);
 }

 @Override
 public void seek(ByteBuffer target) {
   assert (isOwningHandle() && target.isDirect());
   seekDirect0(nativeHandle_, target, target.position(), target.remaining());
   target.position(target.limit());
 }

 @Override
 public void seekForPrev(ByteBuffer target) {
   assert (isOwningHandle() && target.isDirect());
   seekForPrevDirect0(nativeHandle_, target, target.position(), target.remaining());
   target.position(target.limit());
 }

  @Override
  public void next() {
    assert (isOwningHandle());
    next0(nativeHandle_);
  }

  @Override
  public void prev() {
    assert (isOwningHandle());
    prev0(nativeHandle_);
  }

  @Override
  public void refresh() throws CabinDBException {
    assert (isOwningHandle());
    refresh0(nativeHandle_);
  }

  @Override
  public void status() throws CabinDBException {
    assert (isOwningHandle());
    status0(nativeHandle_);
  }

  /**
   * <p>Deletes underlying C++ iterator pointer.</p>
   *
   * <p>Note: the underlying handle can only be safely deleted if the parent
   * instance related to a certain CabinIterator is still valid and initialized.
   * Therefore {@code disposeInternal()} checks if the parent is initialized
   * before freeing the native handle.</p>
   */
  @Override
  protected void disposeInternal() {
      if (parent_.isOwningHandle()) {
        disposeInternal(nativeHandle_);
      }
  }

  abstract boolean isValid0(long handle);
  abstract void seekToFirst0(long handle);
  abstract void seekToLast0(long handle);
  abstract void next0(long handle);
  abstract void prev0(long handle);
  abstract void refresh0(long handle) throws CabinDBException;
  abstract void seek0(long handle, byte[] target, int targetLen);
  abstract void seekForPrev0(long handle, byte[] target, int targetLen);
  abstract void seekDirect0(long handle, ByteBuffer target, int targetOffset, int targetLen);
  abstract void seekForPrevDirect0(long handle, ByteBuffer target, int targetOffset, int targetLen);
  abstract void status0(long handle) throws CabinDBException;
}
