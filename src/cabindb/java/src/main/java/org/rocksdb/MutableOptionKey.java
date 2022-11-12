// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
package org.cabindb;

public interface MutableOptionKey {
  enum ValueType {
    DOUBLE,
    LONG,
    INT,
    BOOLEAN,
    INT_ARRAY,
    ENUM
  }

  String name();
  ValueType getValueType();
}
