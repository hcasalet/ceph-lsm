// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.cabindb;

/**
 * <p>This class provides a custom logger functionality
 * in Java which wraps {@code CabinDB} logging facilities.
 * </p>
 *
 * <p>Using this class CabinDB can log with common
 * Java logging APIs like Log4j or Slf4j without keeping
 * database logs in the filesystem.</p>
 *
 * <strong>Performance</strong>
 * <p>There are certain performance penalties using a Java
 * {@code Logger} implementation within production code.
 * </p>
 *
 * <p>
 * A log level can be set using {@link org.cabindb.Options} or
 * {@link Logger#setInfoLogLevel(InfoLogLevel)}. The set log level
 * influences the underlying native code. Each log message is
 * checked against the set log level and if the log level is more
 * verbose as the set log level, native allocations will be made
 * and data structures are allocated.
 * </p>
 *
 * <p>Every log message which will be emitted by native code will
 * trigger expensive native to Java transitions. So the preferred
 * setting for production use is either
 * {@link org.cabindb.InfoLogLevel#ERROR_LEVEL} or
 * {@link org.cabindb.InfoLogLevel#FATAL_LEVEL}.
 * </p>
 */
public abstract class Logger extends CabinCallbackObject {

  private final static long WITH_OPTIONS = 0;
  private final static long WITH_DBOPTIONS = 1;

  /**
   * <p>AbstractLogger constructor.</p>
   *
   * <p><strong>Important:</strong> the log level set within
   * the {@link org.cabindb.Options} instance will be used as
   * maximum log level of CabinDB.</p>
   *
   * @param options {@link org.cabindb.Options} instance.
   */
  public Logger(final Options options) {
    super(options.nativeHandle_, WITH_OPTIONS);

  }

  /**
   * <p>AbstractLogger constructor.</p>
   *
   * <p><strong>Important:</strong> the log level set within
   * the {@link org.cabindb.DBOptions} instance will be used
   * as maximum log level of CabinDB.</p>
   *
   * @param dboptions {@link org.cabindb.DBOptions} instance.
   */
  public Logger(final DBOptions dboptions) {
    super(dboptions.nativeHandle_, WITH_DBOPTIONS);
  }

  @Override
  protected long initializeNative(long... nativeParameterHandles) {
    if(nativeParameterHandles[1] == WITH_OPTIONS) {
      return createNewLoggerOptions(nativeParameterHandles[0]);
    } else if(nativeParameterHandles[1] == WITH_DBOPTIONS) {
      return createNewLoggerDbOptions(nativeParameterHandles[0]);
    } else {
      throw new IllegalArgumentException();
    }
  }

  /**
   * Set {@link org.cabindb.InfoLogLevel} to AbstractLogger.
   *
   * @param infoLogLevel {@link org.cabindb.InfoLogLevel} instance.
   */
  public void setInfoLogLevel(final InfoLogLevel infoLogLevel) {
      setInfoLogLevel(nativeHandle_, infoLogLevel.getValue());
  }

  /**
   * Return the loggers log level.
   *
   * @return {@link org.cabindb.InfoLogLevel} instance.
   */
  public InfoLogLevel infoLogLevel() {
    return InfoLogLevel.getInfoLogLevel(
        infoLogLevel(nativeHandle_));
  }

  protected abstract void log(InfoLogLevel infoLogLevel,
      String logMsg);

  protected native long createNewLoggerOptions(
      long options);
  protected native long createNewLoggerDbOptions(
      long dbOptions);
  protected native void setInfoLogLevel(long handle,
      byte infoLogLevel);
  protected native byte infoLogLevel(long handle);

  /**
   * We override {@link CabinCallbackObject#disposeInternal()}
   * as disposing of a cabindb::LoggerJniCallback requires
   * a slightly different approach as it is a std::shared_ptr
   */
  @Override
  protected void disposeInternal() {
    disposeInternal(nativeHandle_);
  }

  private native void disposeInternal(final long handle);
}
