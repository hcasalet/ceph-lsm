// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.cabindb;

/**
 * A CabinDBException encapsulates the error of an operation.  This exception
 * type is used to describe an internal error from the c++ cabindb library.
 */
public class CabinDBException extends Exception {

  /* @Nullable */ private final Status status;

  /**
   * The private construct used by a set of public static factory method.
   *
   * @param msg the specified error message.
   */
  public CabinDBException(final String msg) {
    this(msg, null);
  }

  public CabinDBException(final String msg, final Status status) {
    super(msg);
    this.status = status;
  }

  public CabinDBException(final Status status) {
    super(status.getState() != null ? status.getState()
        : status.getCodeString());
    this.status = status;
  }

  /**
   * Get the status returned from CabinDB
   *
   * @return The status reported by CabinDB, or null if no status is available
   */
  public Status getStatus() {
    return status;
  }
}
