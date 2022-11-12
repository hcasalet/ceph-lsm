// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.cabindb;

import java.util.List;

/**
 * CabinCallbackObject is similar to {@link CabinObject} but varies
 * in its construction as it is designed for Java objects which have functions
 * which are called from C++ via JNI.
 *
 * CabinCallbackObject is the base-class any CabinDB classes that acts as a
 * callback from some underlying underlying native C++ {@code cabindb} object.
 *
 * The use of {@code CabinObject} should always be preferred over
 * {@link CabinCallbackObject} if callbacks are not required.
 */
public abstract class CabinCallbackObject extends
    AbstractImmutableNativeReference {

  protected final long nativeHandle_;

  protected CabinCallbackObject(final long... nativeParameterHandles) {
    super(true);
    this.nativeHandle_ = initializeNative(nativeParameterHandles);
  }

  /**
   * Given a list of CabinCallbackObjects, it returns a list
   * of the native handles of the underlying objects.
   *
   * @param objectList the cabin callback objects
   *
   * @return the native handles
   */
  static /* @Nullable */ long[] toNativeHandleList(
      /* @Nullable */ final List<? extends CabinCallbackObject> objectList) {
    if (objectList == null) {
      return null;
    }
    final int len = objectList.size();
    final long[] handleList = new long[len];
    for (int i = 0; i < len; i++) {
      handleList[i] = objectList.get(i).nativeHandle_;
    }
    return handleList;
  }

  /**
   * Construct the Native C++ object which will callback
   * to our object methods
   *
   * @param nativeParameterHandles An array of native handles for any parameter
   *     objects that are needed during construction
   *
   * @return The native handle of the C++ object which will callback to us
   */
  protected abstract long initializeNative(
      final long... nativeParameterHandles);

  /**
   * Deletes underlying C++ native callback object pointer
   */
  @Override
  protected void disposeInternal() {
    disposeInternal(nativeHandle_);
  }

  private native void disposeInternal(final long handle);
}
