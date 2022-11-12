// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
package org.cabindb;

/**
 * Base class for Table Filters.
 */
public abstract class AbstractTableFilter
    extends CabinCallbackObject implements TableFilter {

  protected AbstractTableFilter() {
    super();
  }

  @Override
  protected long initializeNative(final long... nativeParameterHandles) {
    return createNewTableFilter();
  }

  private native long createNewTableFilter();
}
