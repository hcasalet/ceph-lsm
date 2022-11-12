// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.cabindb;

import org.junit.Test;

import org.cabindb.Status.Code;
import org.cabindb.Status.SubCode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class CabinDBExceptionTest {

  @Test
  public void exception() {
    try {
      raiseException();
    } catch(final CabinDBException e) {
      assertThat(e.getStatus()).isNull();
      assertThat(e.getMessage()).isEqualTo("test message");
      return;
    }
    fail();
  }

  @Test
  public void exceptionWithStatusCode() {
    try {
      raiseExceptionWithStatusCode();
    } catch(final CabinDBException e) {
      assertThat(e.getStatus()).isNotNull();
      assertThat(e.getStatus().getCode()).isEqualTo(Code.NotSupported);
      assertThat(e.getStatus().getSubCode()).isEqualTo(SubCode.None);
      assertThat(e.getStatus().getState()).isNull();
      assertThat(e.getMessage()).isEqualTo("test message");
      return;
    }
    fail();
  }

  @Test
  public void exceptionNoMsgWithStatusCode() {
    try {
      raiseExceptionNoMsgWithStatusCode();
    } catch(final CabinDBException e) {
      assertThat(e.getStatus()).isNotNull();
      assertThat(e.getStatus().getCode()).isEqualTo(Code.NotSupported);
      assertThat(e.getStatus().getSubCode()).isEqualTo(SubCode.None);
      assertThat(e.getStatus().getState()).isNull();
      assertThat(e.getMessage()).isEqualTo(Code.NotSupported.name());
      return;
    }
    fail();
  }

  @Test
  public void exceptionWithStatusCodeSubCode() {
    try {
      raiseExceptionWithStatusCodeSubCode();
    } catch(final CabinDBException e) {
      assertThat(e.getStatus()).isNotNull();
      assertThat(e.getStatus().getCode()).isEqualTo(Code.TimedOut);
      assertThat(e.getStatus().getSubCode())
          .isEqualTo(Status.SubCode.LockTimeout);
      assertThat(e.getStatus().getState()).isNull();
      assertThat(e.getMessage()).isEqualTo("test message");
      return;
    }
    fail();
  }

  @Test
  public void exceptionNoMsgWithStatusCodeSubCode() {
    try {
      raiseExceptionNoMsgWithStatusCodeSubCode();
    } catch(final CabinDBException e) {
      assertThat(e.getStatus()).isNotNull();
      assertThat(e.getStatus().getCode()).isEqualTo(Code.TimedOut);
      assertThat(e.getStatus().getSubCode()).isEqualTo(SubCode.LockTimeout);
      assertThat(e.getStatus().getState()).isNull();
      assertThat(e.getMessage()).isEqualTo(Code.TimedOut.name() +
          "(" + SubCode.LockTimeout.name() + ")");
      return;
    }
    fail();
  }

  @Test
  public void exceptionWithStatusCodeState() {
    try {
      raiseExceptionWithStatusCodeState();
    } catch(final CabinDBException e) {
      assertThat(e.getStatus()).isNotNull();
      assertThat(e.getStatus().getCode()).isEqualTo(Code.NotSupported);
      assertThat(e.getStatus().getSubCode()).isEqualTo(SubCode.None);
      assertThat(e.getStatus().getState()).isNotNull();
      assertThat(e.getMessage()).isEqualTo("test message");
      return;
    }
    fail();
  }

  private native void raiseException() throws CabinDBException;
  private native void raiseExceptionWithStatusCode() throws CabinDBException;
  private native void raiseExceptionNoMsgWithStatusCode() throws CabinDBException;
  private native void raiseExceptionWithStatusCodeSubCode()
      throws CabinDBException;
  private native void raiseExceptionNoMsgWithStatusCodeSubCode()
      throws CabinDBException;
  private native void raiseExceptionWithStatusCodeState()
      throws CabinDBException;
}
