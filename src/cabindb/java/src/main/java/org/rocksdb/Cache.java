// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.cabindb;


public abstract class Cache extends CabinObject {
  protected Cache(final long nativeHandle) {
    super(nativeHandle);
  }
}
