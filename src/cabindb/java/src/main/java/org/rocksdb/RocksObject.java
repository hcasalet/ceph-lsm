// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.cabindb;

/**
 * CabinObject is an implementation of {@link AbstractNativeReference} which
 * has an immutable and therefore thread-safe reference to the underlying
 * native C++ CabinDB object.
 * <p>
 * CabinObject is the base-class of almost all CabinDB classes that have a
 * pointer to some underlying native C++ {@code cabindb} object.</p>
 * <p>
 * The use of {@code CabinObject} should always be preferred over
 * {@link CabinMutableObject}.</p>
 */
public abstract class CabinObject extends AbstractImmutableNativeReference {

  /**
   * An immutable reference to the value of the C++ pointer pointing to some
   * underlying native CabinDB C++ object.
   */
  protected final long nativeHandle_;

  protected CabinObject(final long nativeHandle) {
    super(true);
    this.nativeHandle_ = nativeHandle;
  }

  /**
   * Deletes underlying C++ object pointer.
   */
  @Override
  protected void disposeInternal() {
    disposeInternal(nativeHandle_);
  }

  protected abstract void disposeInternal(final long handle);

  public long getNativeHandle() {
    return nativeHandle_;
  }
}
