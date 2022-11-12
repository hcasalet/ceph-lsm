// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.cabindb;

import java.util.ArrayList;
import java.util.List;

public class OptionsUtil {
  /**
   * A static method to construct the DBOptions and ColumnFamilyDescriptors by
   * loading the latest CabinDB options file stored in the specified cabindb
   * database.
   *
   * Note that the all the pointer options (except table_factory, which will
   * be described in more details below) will be initialized with the default
   * values.  Developers can further initialize them after this function call.
   * Below is an example list of pointer options which will be initialized.
   *
   * - env
   * - memtable_factory
   * - compaction_filter_factory
   * - prefix_extractor
   * - comparator
   * - merge_operator
   * - compaction_filter
   *
   * For table_factory, this function further supports deserializing
   * BlockBasedTableFactory and its BlockBasedTableOptions except the
   * pointer options of BlockBasedTableOptions (flush_block_policy_factory,
   * block_cache, and block_cache_compressed), which will be initialized with
   * default values.  Developers can further specify these three options by
   * casting the return value of TableFactoroy::GetOptions() to
   * BlockBasedTableOptions and making necessary changes.
   *
   * @param dbPath the path to the CabinDB.
   * @param env {@link org.cabindb.Env} instance.
   * @param dbOptions {@link org.cabindb.DBOptions} instance. This will be
   *     filled and returned.
   * @param cfDescs A list of {@link org.cabindb.ColumnFamilyDescriptor}'s be
   *    returned.
   *
   * @throws CabinDBException thrown if error happens in underlying
   *     native library.
   */

  public static void loadLatestOptions(String dbPath, Env env, DBOptions dbOptions,
      List<ColumnFamilyDescriptor> cfDescs) throws CabinDBException {
    loadLatestOptions(dbPath, env, dbOptions, cfDescs, false);
  }

  /**
   * @param dbPath the path to the CabinDB.
   * @param env {@link org.cabindb.Env} instance.
   * @param dbOptions {@link org.cabindb.DBOptions} instance. This will be
   *     filled and returned.
   * @param cfDescs A list of {@link org.cabindb.ColumnFamilyDescriptor}'s be
   *     returned.
   * @param ignoreUnknownOptions this flag can be set to true if you want to
   *     ignore options that are from a newer version of the db, essentially for
   *     forward compatibility.
   *
   * @throws CabinDBException thrown if error happens in underlying
   *     native library.
   */
  public static void loadLatestOptions(String dbPath, Env env, DBOptions dbOptions,
      List<ColumnFamilyDescriptor> cfDescs, boolean ignoreUnknownOptions) throws CabinDBException {
    loadLatestOptions(
        dbPath, env.nativeHandle_, dbOptions.nativeHandle_, cfDescs, ignoreUnknownOptions);
  }

  /**
   * Similar to LoadLatestOptions, this function constructs the DBOptions
   * and ColumnFamilyDescriptors based on the specified CabinDB Options file.
   * See LoadLatestOptions above.
   *
   * @param dbPath the path to the CabinDB.
   * @param configOptions {@link org.cabindb.ConfigOptions} instance.
   * @param dbOptions {@link org.cabindb.DBOptions} instance. This will be
   *     filled and returned.
   * @param cfDescs A list of {@link org.cabindb.ColumnFamilyDescriptor}'s be
   *     returned.
   * @throws CabinDBException thrown if error happens in underlying
   *     native library.
   */
  public static void loadLatestOptions(ConfigOptions configOptions, String dbPath,
      DBOptions dbOptions, List<ColumnFamilyDescriptor> cfDescs) throws CabinDBException {
    loadLatestOptions(configOptions.nativeHandle_, dbPath, dbOptions.nativeHandle_, cfDescs);
  }

  /**
   * Similar to LoadLatestOptions, this function constructs the DBOptions
   * and ColumnFamilyDescriptors based on the specified CabinDB Options file.
   * See LoadLatestOptions above.
   *
   * @param optionsFileName the CabinDB options file path.
   * @param env {@link org.cabindb.Env} instance.
   * @param dbOptions {@link org.cabindb.DBOptions} instance. This will be
   *     filled and returned.
   * @param cfDescs A list of {@link org.cabindb.ColumnFamilyDescriptor}'s be
   *     returned.
   *
   * @throws CabinDBException thrown if error happens in underlying
   *     native library.
   */
  public static void loadOptionsFromFile(String optionsFileName, Env env, DBOptions dbOptions,
      List<ColumnFamilyDescriptor> cfDescs) throws CabinDBException {
    loadOptionsFromFile(optionsFileName, env, dbOptions, cfDescs, false);
  }

  /**
   * @param optionsFileName the CabinDB options file path.
   * @param env {@link org.cabindb.Env} instance.
   * @param dbOptions {@link org.cabindb.DBOptions} instance. This will be
   *     filled and returned.
   * @param cfDescs A list of {@link org.cabindb.ColumnFamilyDescriptor}'s be
   *     returned.
   * @param ignoreUnknownOptions this flag can be set to true if you want to
   *     ignore options that are from a newer version of the db, esentially for
   *     forward compatibility.
   *
   * @throws CabinDBException thrown if error happens in underlying
   *     native library.
   */
  public static void loadOptionsFromFile(String optionsFileName, Env env, DBOptions dbOptions,
      List<ColumnFamilyDescriptor> cfDescs, boolean ignoreUnknownOptions) throws CabinDBException {
    loadOptionsFromFile(
        optionsFileName, env.nativeHandle_, dbOptions.nativeHandle_, cfDescs, ignoreUnknownOptions);
  }

  /**
   * Similar to LoadLatestOptions, this function constructs the DBOptions
   * and ColumnFamilyDescriptors based on the specified CabinDB Options file.
   * See LoadLatestOptions above.
   *
   * @param optionsFileName the CabinDB options file path.
   * @param configOptions {@link org.cabindb.ConfigOptions} instance.
   * @param dbOptions {@link org.cabindb.DBOptions} instance. This will be
   *     filled and returned.
   * @param cfDescs A list of {@link org.cabindb.ColumnFamilyDescriptor}'s be
   *     returned.
   * @throws CabinDBException thrown if error happens in underlying
   *     native library.
   */
  public static void loadOptionsFromFile(ConfigOptions configOptions, String optionsFileName,
      DBOptions dbOptions, List<ColumnFamilyDescriptor> cfDescs) throws CabinDBException {
    loadOptionsFromFile(
        configOptions.nativeHandle_, optionsFileName, dbOptions.nativeHandle_, cfDescs);
  }

  /**
   * Returns the latest options file name under the specified CabinDB path.
   *
   * @param dbPath the path to the CabinDB.
   * @param env {@link org.cabindb.Env} instance.
   * @return the latest options file name under the db path.
   *
   * @throws CabinDBException thrown if error happens in underlying
   *     native library.
   */
  public static String getLatestOptionsFileName(String dbPath, Env env) throws CabinDBException {
    return getLatestOptionsFileName(dbPath, env.nativeHandle_);
  }

  /**
   * Private constructor.
   * This class has only static methods and shouldn't be instantiated.
   */
  private OptionsUtil() {}

  // native methods
  private native static void loadLatestOptions(String dbPath, long envHandle, long dbOptionsHandle,
      List<ColumnFamilyDescriptor> cfDescs, boolean ignoreUnknownOptions) throws CabinDBException;
  private native static void loadLatestOptions(long cfgHandle, String dbPath, long dbOptionsHandle,
      List<ColumnFamilyDescriptor> cfDescs) throws CabinDBException;
  private native static void loadOptionsFromFile(String optionsFileName, long envHandle,
      long dbOptionsHandle, List<ColumnFamilyDescriptor> cfDescs, boolean ignoreUnknownOptions)
      throws CabinDBException;
  private native static void loadOptionsFromFile(long cfgHandle, String optionsFileName,
      long dbOptionsHandle, List<ColumnFamilyDescriptor> cfDescs) throws CabinDBException;
  private native static String getLatestOptionsFileName(String dbPath, long envHandle)
      throws CabinDBException;
}
