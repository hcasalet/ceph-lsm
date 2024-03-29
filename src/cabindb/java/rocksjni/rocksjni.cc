// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ CABINDB_NAMESPACE::DB methods from Java side.

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "include/org_cabindb_CabinDB.h"
#include "cabindb/cache.h"
#include "cabindb/convenience.h"
#include "cabindb/db.h"
#include "cabindb/options.h"
#include "cabindb/types.h"
#include "cabindb/version.h"
#include "cabinjni/portal.h"

#ifdef min
#undef min
#endif

jlong cabindb_open_helper(JNIEnv* env, jlong jopt_handle, jstring jdb_path,
                          std::function<CABINDB_NAMESPACE::Status(
                              const CABINDB_NAMESPACE::Options&,
                              const std::string&, CABINDB_NAMESPACE::DB**)>
                              open_fn) {
  const char* db_path = env->GetStringUTFChars(jdb_path, nullptr);
  if (db_path == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }

  auto* opt = reinterpret_cast<CABINDB_NAMESPACE::Options*>(jopt_handle);
  CABINDB_NAMESPACE::DB* db = nullptr;
  CABINDB_NAMESPACE::Status s = open_fn(*opt, db_path, &db);

  env->ReleaseStringUTFChars(jdb_path, db_path);

  if (s.ok()) {
    return reinterpret_cast<jlong>(db);
  } else {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
    return 0;
  }
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    open
 * Signature: (JLjava/lang/String;)J
 */
jlong Java_org_cabindb_CabinDB_open__JLjava_lang_String_2(
    JNIEnv* env, jclass, jlong jopt_handle, jstring jdb_path) {
  return cabindb_open_helper(env, jopt_handle, jdb_path,
                             (CABINDB_NAMESPACE::Status(*)(
                                 const CABINDB_NAMESPACE::Options&,
                                 const std::string&, CABINDB_NAMESPACE::DB**)) &
                                 CABINDB_NAMESPACE::DB::Open);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    openROnly
 * Signature: (JLjava/lang/String;Z)J
 */
jlong Java_org_cabindb_CabinDB_openROnly__JLjava_lang_String_2Z(
    JNIEnv* env, jclass, jlong jopt_handle, jstring jdb_path,
    jboolean jerror_if_wal_file_exists) {
  const bool error_if_wal_file_exists = jerror_if_wal_file_exists == JNI_TRUE;
  return cabindb_open_helper(
      env, jopt_handle, jdb_path,
      [error_if_wal_file_exists](const CABINDB_NAMESPACE::Options& options,
                                 const std::string& db_path,
                                 CABINDB_NAMESPACE::DB** db) {
        return CABINDB_NAMESPACE::DB::OpenForReadOnly(options, db_path, db,
                                                      error_if_wal_file_exists);
      });
}

jlongArray cabindb_open_helper(
    JNIEnv* env, jlong jopt_handle, jstring jdb_path,
    jobjectArray jcolumn_names, jlongArray jcolumn_options,
    std::function<CABINDB_NAMESPACE::Status(
        const CABINDB_NAMESPACE::DBOptions&, const std::string&,
        const std::vector<CABINDB_NAMESPACE::ColumnFamilyDescriptor>&,
        std::vector<CABINDB_NAMESPACE::ColumnFamilyHandle*>*,
        CABINDB_NAMESPACE::DB**)>
        open_fn) {
  const char* db_path = env->GetStringUTFChars(jdb_path, nullptr);
  if (db_path == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  const jsize len_cols = env->GetArrayLength(jcolumn_names);
  jlong* jco = env->GetLongArrayElements(jcolumn_options, nullptr);
  if (jco == nullptr) {
    // exception thrown: OutOfMemoryError
    env->ReleaseStringUTFChars(jdb_path, db_path);
    return nullptr;
  }

  std::vector<CABINDB_NAMESPACE::ColumnFamilyDescriptor> column_families;
  jboolean has_exception = JNI_FALSE;
  CABINDB_NAMESPACE::JniUtil::byteStrings<std::string>(
      env, jcolumn_names,
      [](const char* str_data, const size_t str_len) {
        return std::string(str_data, str_len);
      },
      [&jco, &column_families](size_t idx, std::string cf_name) {
        CABINDB_NAMESPACE::ColumnFamilyOptions* cf_options =
            reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyOptions*>(jco[idx]);
        column_families.push_back(
            CABINDB_NAMESPACE::ColumnFamilyDescriptor(cf_name, *cf_options));
      },
      &has_exception);

  env->ReleaseLongArrayElements(jcolumn_options, jco, JNI_ABORT);

  if (has_exception == JNI_TRUE) {
    // exception occurred
    env->ReleaseStringUTFChars(jdb_path, db_path);
    return nullptr;
  }

  auto* opt = reinterpret_cast<CABINDB_NAMESPACE::DBOptions*>(jopt_handle);
  std::vector<CABINDB_NAMESPACE::ColumnFamilyHandle*> cf_handles;
  CABINDB_NAMESPACE::DB* db = nullptr;
  CABINDB_NAMESPACE::Status s =
      open_fn(*opt, db_path, column_families, &cf_handles, &db);

  // we have now finished with db_path
  env->ReleaseStringUTFChars(jdb_path, db_path);

  // check if open operation was successful
  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }

  const jsize resultsLen = 1 + len_cols;  // db handle + column family handles
  std::unique_ptr<jlong[]> results =
      std::unique_ptr<jlong[]>(new jlong[resultsLen]);
  results[0] = reinterpret_cast<jlong>(db);
  for (int i = 1; i <= len_cols; i++) {
    results[i] = reinterpret_cast<jlong>(cf_handles[i - 1]);
  }

  jlongArray jresults = env->NewLongArray(resultsLen);
  if (jresults == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  env->SetLongArrayRegion(jresults, 0, resultsLen, results.get());
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(jresults);
    return nullptr;
  }

  return jresults;
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    openROnly
 * Signature: (JLjava/lang/String;[[B[JZ)[J
 */
jlongArray Java_org_cabindb_CabinDB_openROnly__JLjava_lang_String_2_3_3B_3JZ(
    JNIEnv* env, jclass, jlong jopt_handle, jstring jdb_path,
    jobjectArray jcolumn_names, jlongArray jcolumn_options,
    jboolean jerror_if_wal_file_exists) {
  const bool error_if_wal_file_exists = jerror_if_wal_file_exists == JNI_TRUE;
  return cabindb_open_helper(
      env, jopt_handle, jdb_path, jcolumn_names, jcolumn_options,
      [error_if_wal_file_exists](
          const CABINDB_NAMESPACE::DBOptions& options,
          const std::string& db_path,
          const std::vector<CABINDB_NAMESPACE::ColumnFamilyDescriptor>&
              column_families,
          std::vector<CABINDB_NAMESPACE::ColumnFamilyHandle*>* handles,
          CABINDB_NAMESPACE::DB** db) {
        return CABINDB_NAMESPACE::DB::OpenForReadOnly(
            options, db_path, column_families, handles, db,
            error_if_wal_file_exists);
      });
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    open
 * Signature: (JLjava/lang/String;[[B[J)[J
 */
jlongArray Java_org_cabindb_CabinDB_open__JLjava_lang_String_2_3_3B_3J(
    JNIEnv* env, jclass, jlong jopt_handle, jstring jdb_path,
    jobjectArray jcolumn_names, jlongArray jcolumn_options) {
  return cabindb_open_helper(
      env, jopt_handle, jdb_path, jcolumn_names, jcolumn_options,
      (CABINDB_NAMESPACE::Status(*)(
          const CABINDB_NAMESPACE::DBOptions&, const std::string&,
          const std::vector<CABINDB_NAMESPACE::ColumnFamilyDescriptor>&,
          std::vector<CABINDB_NAMESPACE::ColumnFamilyHandle*>*,
          CABINDB_NAMESPACE::DB**)) &
          CABINDB_NAMESPACE::DB::Open);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    openAsSecondary
 * Signature: (JLjava/lang/String;Ljava/lang/String;)J
 */
jlong Java_org_cabindb_CabinDB_openAsSecondary__JLjava_lang_String_2Ljava_lang_String_2(
    JNIEnv* env, jclass, jlong jopt_handle, jstring jdb_path,
    jstring jsecondary_db_path) {
  const char* secondary_db_path =
      env->GetStringUTFChars(jsecondary_db_path, nullptr);
  if (secondary_db_path == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }

  jlong db_handle = cabindb_open_helper(
      env, jopt_handle, jdb_path,
      [secondary_db_path](const CABINDB_NAMESPACE::Options& options,
                          const std::string& db_path,
                          CABINDB_NAMESPACE::DB** db) {
        return CABINDB_NAMESPACE::DB::OpenAsSecondary(options, db_path,
                                                      secondary_db_path, db);
      });

  // we have now finished with secondary_db_path
  env->ReleaseStringUTFChars(jsecondary_db_path, secondary_db_path);

  return db_handle;
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    openAsSecondary
 * Signature: (JLjava/lang/String;Ljava/lang/String;[[B[J)[J
 */
jlongArray
Java_org_cabindb_CabinDB_openAsSecondary__JLjava_lang_String_2Ljava_lang_String_2_3_3B_3J(
    JNIEnv* env, jclass, jlong jopt_handle, jstring jdb_path,
    jstring jsecondary_db_path, jobjectArray jcolumn_names,
    jlongArray jcolumn_options) {
  const char* secondary_db_path =
      env->GetStringUTFChars(jsecondary_db_path, nullptr);
  if (secondary_db_path == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  jlongArray jhandles = cabindb_open_helper(
      env, jopt_handle, jdb_path, jcolumn_names, jcolumn_options,
      [secondary_db_path](
          const CABINDB_NAMESPACE::DBOptions& options,
          const std::string& db_path,
          const std::vector<CABINDB_NAMESPACE::ColumnFamilyDescriptor>&
              column_families,
          std::vector<CABINDB_NAMESPACE::ColumnFamilyHandle*>* handles,
          CABINDB_NAMESPACE::DB** db) {
        return CABINDB_NAMESPACE::DB::OpenAsSecondary(
            options, db_path, secondary_db_path, column_families, handles, db);
      });

  // we have now finished with secondary_db_path
  env->ReleaseStringUTFChars(jsecondary_db_path, secondary_db_path);

  return jhandles;
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_cabindb_CabinDB_disposeInternal(
    JNIEnv*, jobject, jlong jhandle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jhandle);
  assert(db != nullptr);
  delete db;
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    closeDatabase
 * Signature: (J)V
 */
void Java_org_cabindb_CabinDB_closeDatabase(
    JNIEnv* env, jclass, jlong jhandle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jhandle);
  assert(db != nullptr);
  CABINDB_NAMESPACE::Status s = db->Close();
  CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    listColumnFamilies
 * Signature: (JLjava/lang/String;)[[B
 */
jobjectArray Java_org_cabindb_CabinDB_listColumnFamilies(
    JNIEnv* env, jclass, jlong jopt_handle, jstring jdb_path) {
  std::vector<std::string> column_family_names;
  const char* db_path = env->GetStringUTFChars(jdb_path, nullptr);
  if (db_path == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  auto* opt = reinterpret_cast<CABINDB_NAMESPACE::Options*>(jopt_handle);
  CABINDB_NAMESPACE::Status s = CABINDB_NAMESPACE::DB::ListColumnFamilies(
      *opt, db_path, &column_family_names);

  env->ReleaseStringUTFChars(jdb_path, db_path);

  jobjectArray jcolumn_family_names =
      CABINDB_NAMESPACE::JniUtil::stringsBytes(env, column_family_names);

  return jcolumn_family_names;
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    createColumnFamily
 * Signature: (J[BIJ)J
 */
jlong Java_org_cabindb_CabinDB_createColumnFamily(
    JNIEnv* env, jobject, jlong jhandle, jbyteArray jcf_name,
    jint jcf_name_len, jlong jcf_options_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jhandle);
  jboolean has_exception = JNI_FALSE;
  const std::string cf_name =
      CABINDB_NAMESPACE::JniUtil::byteString<std::string>(
          env, jcf_name, jcf_name_len,
          [](const char* str, const size_t len) {
            return std::string(str, len);
          },
          &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return 0;
  }
  auto* cf_options = reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyOptions*>(
      jcf_options_handle);
  CABINDB_NAMESPACE::ColumnFamilyHandle* cf_handle;
  CABINDB_NAMESPACE::Status s =
      db->CreateColumnFamily(*cf_options, cf_name, &cf_handle);
  if (!s.ok()) {
    // error occurred
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
    return 0;
  }
  return reinterpret_cast<jlong>(cf_handle);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    createColumnFamilies
 * Signature: (JJ[[B)[J
 */
jlongArray Java_org_cabindb_CabinDB_createColumnFamilies__JJ_3_3B(
    JNIEnv* env, jobject, jlong jhandle, jlong jcf_options_handle,
    jobjectArray jcf_names) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jhandle);
  auto* cf_options = reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyOptions*>(
      jcf_options_handle);
  jboolean has_exception = JNI_FALSE;
  std::vector<std::string> cf_names;
  CABINDB_NAMESPACE::JniUtil::byteStrings<std::string>(
      env, jcf_names,
      [](const char* str, const size_t len) { return std::string(str, len); },
      [&cf_names](const size_t, std::string str) { cf_names.push_back(str); },
      &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return nullptr;
  }

  std::vector<CABINDB_NAMESPACE::ColumnFamilyHandle*> cf_handles;
  CABINDB_NAMESPACE::Status s =
      db->CreateColumnFamilies(*cf_options, cf_names, &cf_handles);
  if (!s.ok()) {
    // error occurred
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }

  jlongArray jcf_handles = CABINDB_NAMESPACE::JniUtil::toJPointers<
      CABINDB_NAMESPACE::ColumnFamilyHandle>(env, cf_handles, &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return nullptr;
  }
  return jcf_handles;
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    createColumnFamilies
 * Signature: (J[J[[B)[J
 */
jlongArray Java_org_cabindb_CabinDB_createColumnFamilies__J_3J_3_3B(
    JNIEnv* env, jobject, jlong jhandle, jlongArray jcf_options_handles,
    jobjectArray jcf_names) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jhandle);
  const jsize jlen = env->GetArrayLength(jcf_options_handles);
  std::vector<CABINDB_NAMESPACE::ColumnFamilyDescriptor> cf_descriptors;
  cf_descriptors.reserve(jlen);

  jlong* jcf_options_handles_elems =
      env->GetLongArrayElements(jcf_options_handles, nullptr);
  if(jcf_options_handles_elems == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
  }

  // extract the column family descriptors
  jboolean has_exception = JNI_FALSE;
  for (jsize i = 0; i < jlen; i++) {
    auto* cf_options =
        reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyOptions*>(
            jcf_options_handles_elems[i]);
    jbyteArray jcf_name = static_cast<jbyteArray>(
        env->GetObjectArrayElement(jcf_names, i));
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->ReleaseLongArrayElements(jcf_options_handles, jcf_options_handles_elems, JNI_ABORT);
      return nullptr;
    }
    const std::string cf_name =
        CABINDB_NAMESPACE::JniUtil::byteString<std::string>(
            env, jcf_name,
            [](const char* str, const size_t len) {
              return std::string(str, len);
            },
            &has_exception);
    if (has_exception == JNI_TRUE) {
      // exception occurred
      env->DeleteLocalRef(jcf_name);
      env->ReleaseLongArrayElements(jcf_options_handles, jcf_options_handles_elems, JNI_ABORT);
      return nullptr;
    }

    cf_descriptors.push_back(
        CABINDB_NAMESPACE::ColumnFamilyDescriptor(cf_name, *cf_options));

    env->DeleteLocalRef(jcf_name);
  }

  std::vector<CABINDB_NAMESPACE::ColumnFamilyHandle*> cf_handles;
  CABINDB_NAMESPACE::Status s =
      db->CreateColumnFamilies(cf_descriptors, &cf_handles);

  env->ReleaseLongArrayElements(jcf_options_handles, jcf_options_handles_elems, JNI_ABORT);

  if (!s.ok()) {
    // error occurred
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }

  jlongArray jcf_handles = CABINDB_NAMESPACE::JniUtil::toJPointers<
      CABINDB_NAMESPACE::ColumnFamilyHandle>(env, cf_handles, &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return nullptr;
  }
  return jcf_handles;
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    dropColumnFamily
 * Signature: (JJ)V;
 */
void Java_org_cabindb_CabinDB_dropColumnFamily(
    JNIEnv* env, jobject, jlong jdb_handle,
    jlong jcf_handle) {
  auto* db_handle = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto* cf_handle =
      reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  CABINDB_NAMESPACE::Status s = db_handle->DropColumnFamily(cf_handle);
  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    dropColumnFamilies
 * Signature: (J[J)V
 */
void Java_org_cabindb_CabinDB_dropColumnFamilies(
    JNIEnv* env, jobject, jlong jdb_handle,
    jlongArray jcolumn_family_handles) {
  auto* db_handle = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);

  std::vector<CABINDB_NAMESPACE::ColumnFamilyHandle*> cf_handles;
  if (jcolumn_family_handles != nullptr) {
    const jsize len_cols = env->GetArrayLength(jcolumn_family_handles);

    jlong* jcfh = env->GetLongArrayElements(jcolumn_family_handles, nullptr);
    if (jcfh == nullptr) {
      // exception thrown: OutOfMemoryError
      return;
    }

    for (jsize i = 0; i < len_cols; i++) {
      auto* cf_handle =
          reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcfh[i]);
      cf_handles.push_back(cf_handle);
    }
    env->ReleaseLongArrayElements(jcolumn_family_handles, jcfh, JNI_ABORT);
  }

  CABINDB_NAMESPACE::Status s = db_handle->DropColumnFamilies(cf_handles);
  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  }
}

//////////////////////////////////////////////////////////////////////////////
// CABINDB_NAMESPACE::DB::Put

/**
 * @return true if the put succeeded, false if a Java Exception was thrown
 */
bool cabindb_put_helper(JNIEnv* env, CABINDB_NAMESPACE::DB* db,
                        const CABINDB_NAMESPACE::WriteOptions& write_options,
                        CABINDB_NAMESPACE::ColumnFamilyHandle* cf_handle,
                        jbyteArray jkey, jint jkey_off, jint jkey_len,
                        jbyteArray jval, jint jval_off, jint jval_len) {
  jbyte* key = new jbyte[jkey_len];
  env->GetByteArrayRegion(jkey, jkey_off, jkey_len, key);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] key;
    return false;
  }

  jbyte* value = new jbyte[jval_len];
  env->GetByteArrayRegion(jval, jval_off, jval_len, value);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] value;
    delete[] key;
    return false;
  }

  CABINDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);
  CABINDB_NAMESPACE::Slice value_slice(reinterpret_cast<char*>(value),
                                       jval_len);

  CABINDB_NAMESPACE::Status s;
  if (cf_handle != nullptr) {
    s = db->Put(write_options, cf_handle, key_slice, value_slice);
  } else {
    // backwards compatibility
    s = db->Put(write_options, key_slice, value_slice);
  }

  // cleanup
  delete[] value;
  delete[] key;

  if (s.ok()) {
    return true;
  } else {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
    return false;
  }
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    put
 * Signature: (J[BII[BII)V
 */
void Java_org_cabindb_CabinDB_put__J_3BII_3BII(
    JNIEnv* env, jobject, jlong jdb_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len,
    jbyteArray jval, jint jval_off, jint jval_len) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  static const CABINDB_NAMESPACE::WriteOptions default_write_options =
      CABINDB_NAMESPACE::WriteOptions();
  cabindb_put_helper(env, db, default_write_options, nullptr, jkey, jkey_off,
      jkey_len, jval, jval_off, jval_len);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    put
 * Signature: (J[BII[BIIJ)V
 */
void Java_org_cabindb_CabinDB_put__J_3BII_3BIIJ(
    JNIEnv* env, jobject, jlong jdb_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len,
    jbyteArray jval, jint jval_off, jint jval_len,
    jlong jcf_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  static const CABINDB_NAMESPACE::WriteOptions default_write_options =
      CABINDB_NAMESPACE::WriteOptions();
  auto* cf_handle =
      reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    cabindb_put_helper(env, db, default_write_options, cf_handle, jkey,
        jkey_off, jkey_len, jval, jval_off, jval_len);
  } else {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(
        env, CABINDB_NAMESPACE::Status::InvalidArgument(
                 "Invalid ColumnFamilyHandle."));
  }
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    put
 * Signature: (JJ[BII[BII)V
 */
void Java_org_cabindb_CabinDB_put__JJ_3BII_3BII(
    JNIEnv* env, jobject, jlong jdb_handle,
    jlong jwrite_options_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len,
    jbyteArray jval, jint jval_off, jint jval_len) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<CABINDB_NAMESPACE::WriteOptions*>(jwrite_options_handle);
  cabindb_put_helper(env, db, *write_options, nullptr, jkey, jkey_off, jkey_len,
      jval, jval_off, jval_len);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    put
 * Signature: (JJ[BII[BIIJ)V
 */
void Java_org_cabindb_CabinDB_put__JJ_3BII_3BIIJ(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jwrite_options_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len,
    jbyteArray jval, jint jval_off, jint jval_len,
    jlong jcf_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<CABINDB_NAMESPACE::WriteOptions*>(jwrite_options_handle);
  auto* cf_handle =
      reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    cabindb_put_helper(env, db, *write_options, cf_handle, jkey, jkey_off,
        jkey_len, jval, jval_off, jval_len);
  } else {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(
        env, CABINDB_NAMESPACE::Status::InvalidArgument(
                 "Invalid ColumnFamilyHandle."));
  }
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    putDirect
 * Signature: (JJLjava/nio/ByteBuffer;IILjava/nio/ByteBuffer;IIJ)V
 */
void Java_org_cabindb_CabinDB_putDirect(
    JNIEnv* env, jobject /*jdb*/, jlong jdb_handle, jlong jwrite_options_handle,
    jobject jkey, jint jkey_off, jint jkey_len, jobject jval, jint jval_off,
    jint jval_len, jlong jcf_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<CABINDB_NAMESPACE::WriteOptions*>(jwrite_options_handle);
  auto* cf_handle =
      reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  auto put = [&env, &db, &cf_handle, &write_options](
                 CABINDB_NAMESPACE::Slice& key,
                 CABINDB_NAMESPACE::Slice& value) {
    CABINDB_NAMESPACE::Status s;
    if (cf_handle == nullptr) {
      s = db->Put(*write_options, key, value);
    } else {
      s = db->Put(*write_options, cf_handle, key, value);
    }
    if (s.ok()) {
      return;
    }
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  };
  CABINDB_NAMESPACE::JniUtil::kv_op_direct(put, env, jkey, jkey_off, jkey_len,
                                           jval, jval_off, jval_len);
}

//////////////////////////////////////////////////////////////////////////////
// CABINDB_NAMESPACE::DB::Delete()

/**
 * @return true if the delete succeeded, false if a Java Exception was thrown
 */
bool cabindb_delete_helper(JNIEnv* env, CABINDB_NAMESPACE::DB* db,
                           const CABINDB_NAMESPACE::WriteOptions& write_options,
                           CABINDB_NAMESPACE::ColumnFamilyHandle* cf_handle,
                           jbyteArray jkey, jint jkey_off, jint jkey_len) {
  jbyte* key = new jbyte[jkey_len];
  env->GetByteArrayRegion(jkey, jkey_off, jkey_len, key);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] key;
    return false;
  }
  CABINDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);

  CABINDB_NAMESPACE::Status s;
  if (cf_handle != nullptr) {
    s = db->Delete(write_options, cf_handle, key_slice);
  } else {
    // backwards compatibility
    s = db->Delete(write_options, key_slice);
  }

  // cleanup
  delete[] key;

  if (s.ok()) {
    return true;
  }

  CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  return false;
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    delete
 * Signature: (J[BII)V
 */
void Java_org_cabindb_CabinDB_delete__J_3BII(
    JNIEnv* env, jobject, jlong jdb_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  static const CABINDB_NAMESPACE::WriteOptions default_write_options =
      CABINDB_NAMESPACE::WriteOptions();
  cabindb_delete_helper(env, db, default_write_options, nullptr, jkey, jkey_off,
      jkey_len);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    delete
 * Signature: (J[BIIJ)V
 */
void Java_org_cabindb_CabinDB_delete__J_3BIIJ(
    JNIEnv* env, jobject, jlong jdb_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len,
    jlong jcf_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  static const CABINDB_NAMESPACE::WriteOptions default_write_options =
      CABINDB_NAMESPACE::WriteOptions();
  auto* cf_handle =
      reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    cabindb_delete_helper(env, db, default_write_options, cf_handle, jkey,
        jkey_off, jkey_len);
  } else {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(
        env, CABINDB_NAMESPACE::Status::InvalidArgument(
                 "Invalid ColumnFamilyHandle."));
  }
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    delete
 * Signature: (JJ[BII)V
 */
void Java_org_cabindb_CabinDB_delete__JJ_3BII(
    JNIEnv* env, jobject,
    jlong jdb_handle,
    jlong jwrite_options,
    jbyteArray jkey, jint jkey_off, jint jkey_len) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<CABINDB_NAMESPACE::WriteOptions*>(jwrite_options);
  cabindb_delete_helper(env, db, *write_options, nullptr, jkey, jkey_off,
      jkey_len);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    delete
 * Signature: (JJ[BIIJ)V
 */
void Java_org_cabindb_CabinDB_delete__JJ_3BIIJ(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jwrite_options,
    jbyteArray jkey, jint jkey_off, jint jkey_len, jlong jcf_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<CABINDB_NAMESPACE::WriteOptions*>(jwrite_options);
  auto* cf_handle =
      reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    cabindb_delete_helper(env, db, *write_options, cf_handle, jkey, jkey_off,
        jkey_len);
  } else {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(
        env, CABINDB_NAMESPACE::Status::InvalidArgument(
                 "Invalid ColumnFamilyHandle."));
  }
}

//////////////////////////////////////////////////////////////////////////////
// CABINDB_NAMESPACE::DB::SingleDelete()
/**
 * @return true if the single delete succeeded, false if a Java Exception
 *     was thrown
 */
bool cabindb_single_delete_helper(
    JNIEnv* env, CABINDB_NAMESPACE::DB* db,
    const CABINDB_NAMESPACE::WriteOptions& write_options,
    CABINDB_NAMESPACE::ColumnFamilyHandle* cf_handle, jbyteArray jkey,
    jint jkey_len) {
  jbyte* key = env->GetByteArrayElements(jkey, nullptr);
  if (key == nullptr) {
    // exception thrown: OutOfMemoryError
    return false;
  }
  CABINDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);

  CABINDB_NAMESPACE::Status s;
  if (cf_handle != nullptr) {
    s = db->SingleDelete(write_options, cf_handle, key_slice);
  } else {
    // backwards compatibility
    s = db->SingleDelete(write_options, key_slice);
  }

  // trigger java unref on key and value.
  // by passing JNI_ABORT, it will simply release the reference without
  // copying the result back to the java byte array.
  env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);

  if (s.ok()) {
    return true;
  }

  CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  return false;
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    singleDelete
 * Signature: (J[BI)V
 */
void Java_org_cabindb_CabinDB_singleDelete__J_3BI(
    JNIEnv* env, jobject,
    jlong jdb_handle,
    jbyteArray jkey,
    jint jkey_len) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  static const CABINDB_NAMESPACE::WriteOptions default_write_options =
      CABINDB_NAMESPACE::WriteOptions();
  cabindb_single_delete_helper(env, db, default_write_options, nullptr,
      jkey, jkey_len);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    singleDelete
 * Signature: (J[BIJ)V
 */
void Java_org_cabindb_CabinDB_singleDelete__J_3BIJ(
    JNIEnv* env, jobject, jlong jdb_handle,
    jbyteArray jkey, jint jkey_len, jlong jcf_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  static const CABINDB_NAMESPACE::WriteOptions default_write_options =
      CABINDB_NAMESPACE::WriteOptions();
  auto* cf_handle =
      reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    cabindb_single_delete_helper(env, db, default_write_options, cf_handle,
        jkey, jkey_len);
  } else {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(
        env, CABINDB_NAMESPACE::Status::InvalidArgument(
                 "Invalid ColumnFamilyHandle."));
  }
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    singleDelete
 * Signature: (JJ[BIJ)V
 */
void Java_org_cabindb_CabinDB_singleDelete__JJ_3BI(
    JNIEnv* env, jobject, jlong jdb_handle,
    jlong jwrite_options,
    jbyteArray jkey,
    jint jkey_len) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<CABINDB_NAMESPACE::WriteOptions*>(jwrite_options);
  cabindb_single_delete_helper(env, db, *write_options, nullptr, jkey,
      jkey_len);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    singleDelete
 * Signature: (JJ[BIJ)V
 */
void Java_org_cabindb_CabinDB_singleDelete__JJ_3BIJ(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jwrite_options,
    jbyteArray jkey, jint jkey_len, jlong jcf_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<CABINDB_NAMESPACE::WriteOptions*>(jwrite_options);
  auto* cf_handle =
      reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    cabindb_single_delete_helper(env, db, *write_options, cf_handle, jkey,
        jkey_len);
  } else {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(
        env, CABINDB_NAMESPACE::Status::InvalidArgument(
                 "Invalid ColumnFamilyHandle."));
  }
}

//////////////////////////////////////////////////////////////////////////////
// CABINDB_NAMESPACE::DB::DeleteRange()
/**
 * @return true if the delete range succeeded, false if a Java Exception
 *     was thrown
 */
bool cabindb_delete_range_helper(
    JNIEnv* env, CABINDB_NAMESPACE::DB* db,
    const CABINDB_NAMESPACE::WriteOptions& write_options,
    CABINDB_NAMESPACE::ColumnFamilyHandle* cf_handle, jbyteArray jbegin_key,
    jint jbegin_key_off, jint jbegin_key_len, jbyteArray jend_key,
    jint jend_key_off, jint jend_key_len) {
  jbyte* begin_key = new jbyte[jbegin_key_len];
  env->GetByteArrayRegion(jbegin_key, jbegin_key_off, jbegin_key_len,
      begin_key);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] begin_key;
    return false;
  }
  CABINDB_NAMESPACE::Slice begin_key_slice(reinterpret_cast<char*>(begin_key),
                                           jbegin_key_len);

  jbyte* end_key = new jbyte[jend_key_len];
  env->GetByteArrayRegion(jend_key, jend_key_off, jend_key_len, end_key);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] begin_key;
    delete[] end_key;
    return false;
  }
  CABINDB_NAMESPACE::Slice end_key_slice(reinterpret_cast<char*>(end_key),
                                         jend_key_len);

  CABINDB_NAMESPACE::Status s =
      db->DeleteRange(write_options, cf_handle, begin_key_slice, end_key_slice);

  // cleanup
  delete[] begin_key;
  delete[] end_key;

  if (s.ok()) {
    return true;
  }

  CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  return false;
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    deleteRange
 * Signature: (J[BII[BII)V
 */
void Java_org_cabindb_CabinDB_deleteRange__J_3BII_3BII(
    JNIEnv* env, jobject, jlong jdb_handle,
    jbyteArray jbegin_key, jint jbegin_key_off, jint jbegin_key_len,
    jbyteArray jend_key, jint jend_key_off, jint jend_key_len) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  static const CABINDB_NAMESPACE::WriteOptions default_write_options =
      CABINDB_NAMESPACE::WriteOptions();
  cabindb_delete_range_helper(env, db, default_write_options, nullptr,
      jbegin_key, jbegin_key_off, jbegin_key_len,
      jend_key, jend_key_off, jend_key_len);
}

jint cabindb_get_helper_direct(
    JNIEnv* env, CABINDB_NAMESPACE::DB* db,
    const CABINDB_NAMESPACE::ReadOptions& read_options,
    CABINDB_NAMESPACE::ColumnFamilyHandle* column_family_handle, jobject jkey,
    jint jkey_off, jint jkey_len, jobject jval, jint jval_off, jint jval_len,
    bool* has_exception) {
  static const int kNotFound = -1;
  static const int kStatusError = -2;
  static const int kArgumentError = -3;

  char* key = reinterpret_cast<char*>(env->GetDirectBufferAddress(jkey));
  if (key == nullptr) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(
        env,
        "Invalid key argument (argument is not a valid direct ByteBuffer)");
    *has_exception = true;
    return kArgumentError;
  }
  if (env->GetDirectBufferCapacity(jkey) < (jkey_off + jkey_len)) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(
        env,
        "Invalid key argument. Capacity is less than requested region (offset "
        "+ length).");
    *has_exception = true;
    return kArgumentError;
  }

  char* value = reinterpret_cast<char*>(env->GetDirectBufferAddress(jval));
  if (value == nullptr) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(
        env,
        "Invalid value argument (argument is not a valid direct ByteBuffer)");
    *has_exception = true;
    return kArgumentError;
  }

  if (env->GetDirectBufferCapacity(jval) < (jval_off + jval_len)) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(
        env,
        "Invalid value argument. Capacity is less than requested region "
        "(offset + length).");
    *has_exception = true;
    return kArgumentError;
  }

  key += jkey_off;
  value += jval_off;

  CABINDB_NAMESPACE::Slice key_slice(key, jkey_len);

  // TODO(yhchiang): we might save one memory allocation here by adding
  // a DB::Get() function which takes preallocated jbyte* as input.
  std::string cvalue;
  CABINDB_NAMESPACE::Status s;
  if (column_family_handle != nullptr) {
    s = db->Get(read_options, column_family_handle, key_slice, &cvalue);
  } else {
    // backwards compatibility
    s = db->Get(read_options, key_slice, &cvalue);
  }

  if (s.IsNotFound()) {
    *has_exception = false;
    return kNotFound;
  } else if (!s.ok()) {
    *has_exception = true;
    // Here since we are throwing a Java exception from c++ side.
    // As a result, c++ does not know calling this function will in fact
    // throwing an exception.  As a result, the execution flow will
    // not stop here, and codes after this throw will still be
    // executed.
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);

    // Return a dummy const value to avoid compilation error, although
    // java side might not have a chance to get the return value :)
    return kStatusError;
  }

  const jint cvalue_len = static_cast<jint>(cvalue.size());
  const jint length = std::min(jval_len, cvalue_len);

  memcpy(value, cvalue.c_str(), length);

  *has_exception = false;
  return cvalue_len;
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    deleteRange
 * Signature: (J[BII[BIIJ)V
 */
void Java_org_cabindb_CabinDB_deleteRange__J_3BII_3BIIJ(
    JNIEnv* env, jobject, jlong jdb_handle,
    jbyteArray jbegin_key, jint jbegin_key_off, jint jbegin_key_len,
    jbyteArray jend_key, jint jend_key_off, jint jend_key_len,
    jlong jcf_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  static const CABINDB_NAMESPACE::WriteOptions default_write_options =
      CABINDB_NAMESPACE::WriteOptions();
  auto* cf_handle =
      reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    cabindb_delete_range_helper(env, db, default_write_options, cf_handle,
        jbegin_key, jbegin_key_off, jbegin_key_len,
        jend_key, jend_key_off, jend_key_len);
  } else {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(
        env, CABINDB_NAMESPACE::Status::InvalidArgument(
                 "Invalid ColumnFamilyHandle."));
  }
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    deleteRange
 * Signature: (JJ[BII[BII)V
 */
void Java_org_cabindb_CabinDB_deleteRange__JJ_3BII_3BII(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jwrite_options,
    jbyteArray jbegin_key, jint jbegin_key_off, jint jbegin_key_len,
    jbyteArray jend_key, jint jend_key_off, jint jend_key_len) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<CABINDB_NAMESPACE::WriteOptions*>(jwrite_options);
  cabindb_delete_range_helper(env, db, *write_options, nullptr, jbegin_key,
      jbegin_key_off, jbegin_key_len, jend_key,
      jend_key_off, jend_key_len);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    deleteRange
 * Signature: (JJ[BII[BIIJ)V
 */
void Java_org_cabindb_CabinDB_deleteRange__JJ_3BII_3BIIJ(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jwrite_options,
    jbyteArray jbegin_key, jint jbegin_key_off, jint jbegin_key_len,
    jbyteArray jend_key, jint jend_key_off, jint jend_key_len,
    jlong jcf_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<CABINDB_NAMESPACE::WriteOptions*>(jwrite_options);
  auto* cf_handle =
      reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    cabindb_delete_range_helper(env, db, *write_options, cf_handle,
        jbegin_key, jbegin_key_off, jbegin_key_len,
        jend_key, jend_key_off, jend_key_len);
  } else {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(
        env, CABINDB_NAMESPACE::Status::InvalidArgument(
                 "Invalid ColumnFamilyHandle."));
  }
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    getDirect
 * Signature: (JJLjava/nio/ByteBuffer;IILjava/nio/ByteBuffer;IIJ)I
 */
jint Java_org_cabindb_CabinDB_getDirect(JNIEnv* env, jobject /*jdb*/,
                                        jlong jdb_handle, jlong jropt_handle,
                                        jobject jkey, jint jkey_off,
                                        jint jkey_len, jobject jval,
                                        jint jval_off, jint jval_len,
                                        jlong jcf_handle) {
  auto* db_handle = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto* ro_opt =
      reinterpret_cast<CABINDB_NAMESPACE::ReadOptions*>(jropt_handle);
  auto* cf_handle =
      reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  bool has_exception = false;
  return cabindb_get_helper_direct(
      env, db_handle,
      ro_opt == nullptr ? CABINDB_NAMESPACE::ReadOptions() : *ro_opt, cf_handle,
      jkey, jkey_off, jkey_len, jval, jval_off, jval_len, &has_exception);
}

//////////////////////////////////////////////////////////////////////////////
// CABINDB_NAMESPACE::DB::Merge

/**
 * @return true if the merge succeeded, false if a Java Exception was thrown
 */
bool cabindb_merge_helper(JNIEnv* env, CABINDB_NAMESPACE::DB* db,
                          const CABINDB_NAMESPACE::WriteOptions& write_options,
                          CABINDB_NAMESPACE::ColumnFamilyHandle* cf_handle,
                          jbyteArray jkey, jint jkey_off, jint jkey_len,
                          jbyteArray jval, jint jval_off, jint jval_len) {
  jbyte* key = new jbyte[jkey_len];
  env->GetByteArrayRegion(jkey, jkey_off, jkey_len, key);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] key;
    return false;
  }
  CABINDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);

  jbyte* value = new jbyte[jval_len];
  env->GetByteArrayRegion(jval, jval_off, jval_len, value);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] value;
    delete[] key;
    return false;
  }
  CABINDB_NAMESPACE::Slice value_slice(reinterpret_cast<char*>(value),
                                       jval_len);

  CABINDB_NAMESPACE::Status s;
  if (cf_handle != nullptr) {
    s = db->Merge(write_options, cf_handle, key_slice, value_slice);
  } else {
    s = db->Merge(write_options, key_slice, value_slice);
  }

  // cleanup
  delete[] value;
  delete[] key;

  if (s.ok()) {
    return true;
  }

  CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  return false;
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    merge
 * Signature: (J[BII[BII)V
 */
void Java_org_cabindb_CabinDB_merge__J_3BII_3BII(
    JNIEnv* env, jobject, jlong jdb_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len,
    jbyteArray jval, jint jval_off, jint jval_len) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  static const CABINDB_NAMESPACE::WriteOptions default_write_options =
      CABINDB_NAMESPACE::WriteOptions();
  cabindb_merge_helper(env, db, default_write_options, nullptr, jkey, jkey_off,
      jkey_len, jval, jval_off, jval_len);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    merge
 * Signature: (J[BII[BIIJ)V
 */
void Java_org_cabindb_CabinDB_merge__J_3BII_3BIIJ(
    JNIEnv* env, jobject, jlong jdb_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len,
    jbyteArray jval, jint jval_off, jint jval_len,
    jlong jcf_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  static const CABINDB_NAMESPACE::WriteOptions default_write_options =
      CABINDB_NAMESPACE::WriteOptions();
  auto* cf_handle =
      reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    cabindb_merge_helper(env, db, default_write_options, cf_handle, jkey,
        jkey_off, jkey_len, jval, jval_off, jval_len);
  } else {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(
        env, CABINDB_NAMESPACE::Status::InvalidArgument(
                 "Invalid ColumnFamilyHandle."));
  }
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    merge
 * Signature: (JJ[BII[BII)V
 */
void Java_org_cabindb_CabinDB_merge__JJ_3BII_3BII(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jwrite_options_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len,
    jbyteArray jval, jint jval_off, jint jval_len) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<CABINDB_NAMESPACE::WriteOptions*>(jwrite_options_handle);
  cabindb_merge_helper(env, db, *write_options, nullptr, jkey, jkey_off,
      jkey_len, jval, jval_off, jval_len);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    merge
 * Signature: (JJ[BII[BIIJ)V
 */
void Java_org_cabindb_CabinDB_merge__JJ_3BII_3BIIJ(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jwrite_options_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len,
    jbyteArray jval, jint jval_off, jint jval_len, jlong jcf_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<CABINDB_NAMESPACE::WriteOptions*>(jwrite_options_handle);
  auto* cf_handle =
      reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    cabindb_merge_helper(env, db, *write_options, cf_handle, jkey, jkey_off,
        jkey_len, jval, jval_off, jval_len);
  } else {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(
        env, CABINDB_NAMESPACE::Status::InvalidArgument(
                 "Invalid ColumnFamilyHandle."));
  }
}

jlong cabindb_iterator_helper(
    CABINDB_NAMESPACE::DB* db, CABINDB_NAMESPACE::ReadOptions read_options,
    CABINDB_NAMESPACE::ColumnFamilyHandle* cf_handle) {
  CABINDB_NAMESPACE::Iterator* iterator = nullptr;
  if (cf_handle != nullptr) {
    iterator = db->NewIterator(read_options, cf_handle);
  } else {
    iterator = db->NewIterator(read_options);
  }
  return reinterpret_cast<jlong>(iterator);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    deleteDirect
 * Signature: (JJLjava/nio/ByteBuffer;IIJ)V
 */
void Java_org_cabindb_CabinDB_deleteDirect(JNIEnv* env, jobject /*jdb*/,
                                           jlong jdb_handle,
                                           jlong jwrite_options, jobject jkey,
                                           jint jkey_offset, jint jkey_len,
                                           jlong jcf_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<CABINDB_NAMESPACE::WriteOptions*>(jwrite_options);
  auto* cf_handle =
      reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  auto remove = [&env, &db, &write_options,
                 &cf_handle](CABINDB_NAMESPACE::Slice& key) {
    CABINDB_NAMESPACE::Status s;
    if (cf_handle == nullptr) {
      s = db->Delete(*write_options, key);
    } else {
      s = db->Delete(*write_options, cf_handle, key);
    }
    if (s.ok()) {
      return;
    }
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  };
  CABINDB_NAMESPACE::JniUtil::k_op_direct(remove, env, jkey, jkey_offset,
                                          jkey_len);
}

//////////////////////////////////////////////////////////////////////////////
// CABINDB_NAMESPACE::DB::Write
/*
 * Class:     org_cabindb_CabinDB
 * Method:    write0
 * Signature: (JJJ)V
 */
void Java_org_cabindb_CabinDB_write0(
    JNIEnv* env, jobject, jlong jdb_handle,
    jlong jwrite_options_handle, jlong jwb_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<CABINDB_NAMESPACE::WriteOptions*>(jwrite_options_handle);
  auto* wb = reinterpret_cast<CABINDB_NAMESPACE::WriteBatch*>(jwb_handle);

  CABINDB_NAMESPACE::Status s = db->Write(*write_options, wb);

  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    write1
 * Signature: (JJJ)V
 */
void Java_org_cabindb_CabinDB_write1(
    JNIEnv* env, jobject, jlong jdb_handle,
    jlong jwrite_options_handle, jlong jwbwi_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto* write_options =
      reinterpret_cast<CABINDB_NAMESPACE::WriteOptions*>(jwrite_options_handle);
  auto* wbwi =
      reinterpret_cast<CABINDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  auto* wb = wbwi->GetWriteBatch();

  CABINDB_NAMESPACE::Status s = db->Write(*write_options, wb);

  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  }
}

//////////////////////////////////////////////////////////////////////////////
// CABINDB_NAMESPACE::DB::Get

jbyteArray cabindb_get_helper(
    JNIEnv* env, CABINDB_NAMESPACE::DB* db,
    const CABINDB_NAMESPACE::ReadOptions& read_opt,
    CABINDB_NAMESPACE::ColumnFamilyHandle* column_family_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len) {
  jbyte* key = new jbyte[jkey_len];
  env->GetByteArrayRegion(jkey, jkey_off, jkey_len, key);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] key;
    return nullptr;
  }

  CABINDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);

  std::string value;
  CABINDB_NAMESPACE::Status s;
  if (column_family_handle != nullptr) {
    s = db->Get(read_opt, column_family_handle, key_slice, &value);
  } else {
    // backwards compatibility
    s = db->Get(read_opt, key_slice, &value);
  }

  // cleanup
  delete[] key;

  if (s.IsNotFound()) {
    return nullptr;
  }

  if (s.ok()) {
    jbyteArray jret_value = CABINDB_NAMESPACE::JniUtil::copyBytes(env, value);
    if (jret_value == nullptr) {
      // exception occurred
      return nullptr;
    }
    return jret_value;
  }

  CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  return nullptr;
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    get
 * Signature: (J[BII)[B
 */
jbyteArray Java_org_cabindb_CabinDB_get__J_3BII(
    JNIEnv* env, jobject, jlong jdb_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len) {
  return cabindb_get_helper(
      env, reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle),
      CABINDB_NAMESPACE::ReadOptions(), nullptr, jkey, jkey_off, jkey_len);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    get
 * Signature: (J[BIIJ)[B
 */
jbyteArray Java_org_cabindb_CabinDB_get__J_3BIIJ(
    JNIEnv* env, jobject, jlong jdb_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len, jlong jcf_handle) {
  auto db_handle = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto cf_handle =
      reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    return cabindb_get_helper(env, db_handle, CABINDB_NAMESPACE::ReadOptions(),
                              cf_handle, jkey, jkey_off, jkey_len);
  } else {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(
        env, CABINDB_NAMESPACE::Status::InvalidArgument(
                 "Invalid ColumnFamilyHandle."));
    return nullptr;
  }
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    get
 * Signature: (JJ[BII)[B
 */
jbyteArray Java_org_cabindb_CabinDB_get__JJ_3BII(
    JNIEnv* env, jobject,
    jlong jdb_handle, jlong jropt_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len) {
  return cabindb_get_helper(
      env, reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle),
      *reinterpret_cast<CABINDB_NAMESPACE::ReadOptions*>(jropt_handle), nullptr,
      jkey, jkey_off, jkey_len);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    get
 * Signature: (JJ[BIIJ)[B
 */
jbyteArray Java_org_cabindb_CabinDB_get__JJ_3BIIJ(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jropt_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len, jlong jcf_handle) {
  auto* db_handle = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto& ro_opt =
      *reinterpret_cast<CABINDB_NAMESPACE::ReadOptions*>(jropt_handle);
  auto* cf_handle =
      reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    return cabindb_get_helper(
        env, db_handle, ro_opt, cf_handle, jkey, jkey_off, jkey_len);
  } else {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(
        env, CABINDB_NAMESPACE::Status::InvalidArgument(
                 "Invalid ColumnFamilyHandle."));
    return nullptr;
  }
}

jint cabindb_get_helper(
    JNIEnv* env, CABINDB_NAMESPACE::DB* db,
    const CABINDB_NAMESPACE::ReadOptions& read_options,
    CABINDB_NAMESPACE::ColumnFamilyHandle* column_family_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len, jbyteArray jval,
    jint jval_off, jint jval_len, bool* has_exception) {
  static const int kNotFound = -1;
  static const int kStatusError = -2;

  jbyte* key = new jbyte[jkey_len];
  env->GetByteArrayRegion(jkey, jkey_off, jkey_len, key);
  if (env->ExceptionCheck()) {
    // exception thrown: OutOfMemoryError
    delete[] key;
    *has_exception = true;
    return kStatusError;
  }
  CABINDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);

  // TODO(yhchiang): we might save one memory allocation here by adding
  // a DB::Get() function which takes preallocated jbyte* as input.
  std::string cvalue;
  CABINDB_NAMESPACE::Status s;
  if (column_family_handle != nullptr) {
    s = db->Get(read_options, column_family_handle, key_slice, &cvalue);
  } else {
    // backwards compatibility
    s = db->Get(read_options, key_slice, &cvalue);
  }

  // cleanup
  delete[] key;

  if (s.IsNotFound()) {
    *has_exception = false;
    return kNotFound;
  } else if (!s.ok()) {
    *has_exception = true;
    // Here since we are throwing a Java exception from c++ side.
    // As a result, c++ does not know calling this function will in fact
    // throwing an exception.  As a result, the execution flow will
    // not stop here, and codes after this throw will still be
    // executed.
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);

    // Return a dummy const value to avoid compilation error, although
    // java side might not have a chance to get the return value :)
    return kStatusError;
  }

  const jint cvalue_len = static_cast<jint>(cvalue.size());
  const jint length = std::min(jval_len, cvalue_len);

  env->SetByteArrayRegion(
      jval, jval_off, length,
      const_cast<jbyte*>(reinterpret_cast<const jbyte*>(cvalue.c_str())));
  if (env->ExceptionCheck()) {
    // exception thrown: OutOfMemoryError
    *has_exception = true;
    return kStatusError;
  }

  *has_exception = false;
  return cvalue_len;
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    get
 * Signature: (J[BII[BII)I
 */
jint Java_org_cabindb_CabinDB_get__J_3BII_3BII(
    JNIEnv* env, jobject, jlong jdb_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len,
    jbyteArray jval, jint jval_off, jint jval_len) {
  bool has_exception = false;
  return cabindb_get_helper(
      env, reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle),
      CABINDB_NAMESPACE::ReadOptions(), nullptr, jkey, jkey_off, jkey_len, jval,
      jval_off, jval_len, &has_exception);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    get
 * Signature: (J[BII[BIIJ)I
 */
jint Java_org_cabindb_CabinDB_get__J_3BII_3BIIJ(
    JNIEnv* env, jobject, jlong jdb_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len,
    jbyteArray jval, jint jval_off, jint jval_len,
    jlong jcf_handle) {
  auto* db_handle = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto* cf_handle =
      reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    bool has_exception = false;
    return cabindb_get_helper(env, db_handle, CABINDB_NAMESPACE::ReadOptions(),
                              cf_handle, jkey, jkey_off, jkey_len, jval,
                              jval_off, jval_len, &has_exception);
  } else {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(
        env, CABINDB_NAMESPACE::Status::InvalidArgument(
                 "Invalid ColumnFamilyHandle."));
    // will never be evaluated
    return 0;
  }
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    get
 * Signature: (JJ[BII[BII)I
 */
jint Java_org_cabindb_CabinDB_get__JJ_3BII_3BII(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jropt_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len,
    jbyteArray jval, jint jval_off, jint jval_len) {
  bool has_exception = false;
  return cabindb_get_helper(
      env, reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle),
      *reinterpret_cast<CABINDB_NAMESPACE::ReadOptions*>(jropt_handle), nullptr,
      jkey, jkey_off, jkey_len, jval, jval_off, jval_len, &has_exception);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    get
 * Signature: (JJ[BII[BIIJ)I
 */
jint Java_org_cabindb_CabinDB_get__JJ_3BII_3BIIJ(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jropt_handle,
    jbyteArray jkey, jint jkey_off, jint jkey_len,
    jbyteArray jval, jint jval_off, jint jval_len,
    jlong jcf_handle) {
  auto* db_handle = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto& ro_opt =
      *reinterpret_cast<CABINDB_NAMESPACE::ReadOptions*>(jropt_handle);
  auto* cf_handle =
      reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  if (cf_handle != nullptr) {
    bool has_exception = false;
    return cabindb_get_helper(env, db_handle, ro_opt, cf_handle,
        jkey, jkey_off, jkey_len,
        jval, jval_off, jval_len,
        &has_exception);
  } else {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(
        env, CABINDB_NAMESPACE::Status::InvalidArgument(
                 "Invalid ColumnFamilyHandle."));
    // will never be evaluated
    return 0;
  }
}

inline void multi_get_helper_release_keys(
    JNIEnv* env, std::vector<std::pair<jbyte*, jobject>>& keys_to_free) {
  auto end = keys_to_free.end();
  for (auto it = keys_to_free.begin(); it != end; ++it) {
    delete[] it->first;
    env->DeleteLocalRef(it->second);
  }
  keys_to_free.clear();
}

/**
 * cf multi get
 *
 * @return byte[][] of values or nullptr if an exception occurs
 */
jobjectArray multi_get_helper(JNIEnv* env, jobject, CABINDB_NAMESPACE::DB* db,
                              const CABINDB_NAMESPACE::ReadOptions& rOpt,
                              jobjectArray jkeys, jintArray jkey_offs,
                              jintArray jkey_lens,
                              jlongArray jcolumn_family_handles) {
  std::vector<CABINDB_NAMESPACE::ColumnFamilyHandle*> cf_handles;
  if (jcolumn_family_handles != nullptr) {
    const jsize len_cols = env->GetArrayLength(jcolumn_family_handles);

    jlong* jcfh = env->GetLongArrayElements(jcolumn_family_handles, nullptr);
    if (jcfh == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    for (jsize i = 0; i < len_cols; i++) {
      auto* cf_handle =
          reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcfh[i]);
      cf_handles.push_back(cf_handle);
    }
    env->ReleaseLongArrayElements(jcolumn_family_handles, jcfh, JNI_ABORT);
  }

  const jsize len_keys = env->GetArrayLength(jkeys);
  if (env->EnsureLocalCapacity(len_keys) != 0) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  jint* jkey_off = env->GetIntArrayElements(jkey_offs, nullptr);
  if (jkey_off == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  jint* jkey_len = env->GetIntArrayElements(jkey_lens, nullptr);
  if (jkey_len == nullptr) {
    // exception thrown: OutOfMemoryError
    env->ReleaseIntArrayElements(jkey_offs, jkey_off, JNI_ABORT);
    return nullptr;
  }

  std::vector<CABINDB_NAMESPACE::Slice> keys;
  std::vector<std::pair<jbyte*, jobject>> keys_to_free;
  for (jsize i = 0; i < len_keys; i++) {
    jobject jkey = env->GetObjectArrayElement(jkeys, i);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->ReleaseIntArrayElements(jkey_lens, jkey_len, JNI_ABORT);
      env->ReleaseIntArrayElements(jkey_offs, jkey_off, JNI_ABORT);
      multi_get_helper_release_keys(env, keys_to_free);
      return nullptr;
    }

    jbyteArray jkey_ba = reinterpret_cast<jbyteArray>(jkey);

    const jint len_key = jkey_len[i];
    jbyte* key = new jbyte[len_key];
    env->GetByteArrayRegion(jkey_ba, jkey_off[i], len_key, key);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      delete[] key;
      env->DeleteLocalRef(jkey);
      env->ReleaseIntArrayElements(jkey_lens, jkey_len, JNI_ABORT);
      env->ReleaseIntArrayElements(jkey_offs, jkey_off, JNI_ABORT);
      multi_get_helper_release_keys(env, keys_to_free);
      return nullptr;
    }

    CABINDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key), len_key);
    keys.push_back(key_slice);

    keys_to_free.push_back(std::pair<jbyte*, jobject>(key, jkey));
  }

  // cleanup jkey_off and jken_len
  env->ReleaseIntArrayElements(jkey_lens, jkey_len, JNI_ABORT);
  env->ReleaseIntArrayElements(jkey_offs, jkey_off, JNI_ABORT);

  std::vector<std::string> values;
  std::vector<CABINDB_NAMESPACE::Status> s;
  if (cf_handles.size() == 0) {
    s = db->MultiGet(rOpt, keys, &values);
  } else {
    s = db->MultiGet(rOpt, cf_handles, keys, &values);
  }

  // free up allocated byte arrays
  multi_get_helper_release_keys(env, keys_to_free);

  // prepare the results
  jobjectArray jresults = CABINDB_NAMESPACE::ByteJni::new2dByteArray(
      env, static_cast<jsize>(s.size()));
  if (jresults == nullptr) {
    // exception occurred
    return nullptr;
  }

  // TODO(AR) it is not clear to me why EnsureLocalCapacity is needed for the
  //     loop as we cleanup references with env->DeleteLocalRef(jentry_value);
  if (env->EnsureLocalCapacity(static_cast<jint>(s.size())) != 0) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }
  // add to the jresults
  for (std::vector<CABINDB_NAMESPACE::Status>::size_type i = 0; i != s.size();
       i++) {
    if (s[i].ok()) {
      std::string* value = &values[i];
      const jsize jvalue_len = static_cast<jsize>(value->size());
      jbyteArray jentry_value = env->NewByteArray(jvalue_len);
      if (jentry_value == nullptr) {
        // exception thrown: OutOfMemoryError
        return nullptr;
      }

      env->SetByteArrayRegion(
          jentry_value, 0, static_cast<jsize>(jvalue_len),
          const_cast<jbyte*>(reinterpret_cast<const jbyte*>(value->c_str())));
      if (env->ExceptionCheck()) {
        // exception thrown: ArrayIndexOutOfBoundsException
        env->DeleteLocalRef(jentry_value);
        return nullptr;
      }

      env->SetObjectArrayElement(jresults, static_cast<jsize>(i), jentry_value);
      if (env->ExceptionCheck()) {
        // exception thrown: ArrayIndexOutOfBoundsException
        env->DeleteLocalRef(jentry_value);
        return nullptr;
      }

      env->DeleteLocalRef(jentry_value);
    }
  }

  return jresults;
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    multiGet
 * Signature: (J[[B[I[I)[[B
 */
jobjectArray Java_org_cabindb_CabinDB_multiGet__J_3_3B_3I_3I(
    JNIEnv* env, jobject jdb, jlong jdb_handle,
    jobjectArray jkeys, jintArray jkey_offs, jintArray jkey_lens) {
  return multi_get_helper(
      env, jdb, reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle),
      CABINDB_NAMESPACE::ReadOptions(), jkeys, jkey_offs, jkey_lens, nullptr);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    multiGet
 * Signature: (J[[B[I[I[J)[[B
 */
jobjectArray Java_org_cabindb_CabinDB_multiGet__J_3_3B_3I_3I_3J(
    JNIEnv* env, jobject jdb, jlong jdb_handle,
    jobjectArray jkeys, jintArray jkey_offs, jintArray jkey_lens,
    jlongArray jcolumn_family_handles) {
  return multi_get_helper(env, jdb,
                          reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle),
                          CABINDB_NAMESPACE::ReadOptions(), jkeys, jkey_offs,
                          jkey_lens, jcolumn_family_handles);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    multiGet
 * Signature: (JJ[[B[I[I)[[B
 */
jobjectArray Java_org_cabindb_CabinDB_multiGet__JJ_3_3B_3I_3I(
    JNIEnv* env, jobject jdb, jlong jdb_handle, jlong jropt_handle,
    jobjectArray jkeys, jintArray jkey_offs, jintArray jkey_lens) {
  return multi_get_helper(
      env, jdb, reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle),
      *reinterpret_cast<CABINDB_NAMESPACE::ReadOptions*>(jropt_handle), jkeys,
      jkey_offs, jkey_lens, nullptr);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    multiGet
 * Signature: (JJ[[B[I[I[J)[[B
 */
jobjectArray Java_org_cabindb_CabinDB_multiGet__JJ_3_3B_3I_3I_3J(
    JNIEnv* env, jobject jdb, jlong jdb_handle, jlong jropt_handle,
    jobjectArray jkeys, jintArray jkey_offs, jintArray jkey_lens,
    jlongArray jcolumn_family_handles) {
  return multi_get_helper(
      env, jdb, reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle),
      *reinterpret_cast<CABINDB_NAMESPACE::ReadOptions*>(jropt_handle), jkeys,
      jkey_offs, jkey_lens, jcolumn_family_handles);
}

//////////////////////////////////////////////////////////////////////////////
// CABINDB_NAMESPACE::DB::KeyMayExist
bool key_may_exist_helper(JNIEnv* env, jlong jdb_handle, jlong jcf_handle,
  jlong jread_opts_handle,
  jbyteArray jkey, jint jkey_offset, jint jkey_len,
  bool* has_exception, std::string* value, bool* value_found) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  CABINDB_NAMESPACE::ColumnFamilyHandle* cf_handle;
  if (jcf_handle == 0) {
    cf_handle = db->DefaultColumnFamily();
 } else {
   cf_handle =
       reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
 }
 CABINDB_NAMESPACE::ReadOptions read_opts =
     jread_opts_handle == 0
         ? CABINDB_NAMESPACE::ReadOptions()
         : *(reinterpret_cast<CABINDB_NAMESPACE::ReadOptions*>(
               jread_opts_handle));

 jbyte* key = new jbyte[jkey_len];
 env->GetByteArrayRegion(jkey, jkey_offset, jkey_len, key);
 if (env->ExceptionCheck()) {
   // exception thrown: ArrayIndexOutOfBoundsException
   delete[] key;
   *has_exception = true;
   return false;
  }
  CABINDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);

  const bool exists = db->KeyMayExist(
      read_opts, cf_handle, key_slice, value, value_found);

  // cleanup
  delete[] key;

  return exists;
}


/*
 * Class:     org_cabindb_CabinDB
 * Method:    keyMayExist
 * Signature: (JJJ[BII)Z
 */
jboolean Java_org_cabindb_CabinDB_keyMayExist(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jcf_handle,
    jlong jread_opts_handle,
    jbyteArray jkey, jint jkey_offset, jint jkey_len) {

  bool has_exception = false;
  std::string value;
  bool value_found = false;

  const bool exists = key_may_exist_helper(
      env, jdb_handle, jcf_handle, jread_opts_handle,
      jkey, jkey_offset, jkey_len,
      &has_exception, &value, &value_found);

  if (has_exception) {
    // java exception already raised
    return false;
  }

  return static_cast<jboolean>(exists);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    keyMayExistFoundValue
 * Signature: (JJJ[BII)[[B
 */
jobjectArray Java_org_cabindb_CabinDB_keyMayExistFoundValue(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jcf_handle,
    jlong jread_opts_handle,
    jbyteArray jkey, jint jkey_offset, jint jkey_len) {

  bool has_exception = false;
  std::string value;
  bool value_found = false;

  const bool exists = key_may_exist_helper(
      env, jdb_handle, jcf_handle, jread_opts_handle,
      jkey, jkey_offset, jkey_len,
      &has_exception, &value, &value_found);

  if (has_exception) {
    // java exception already raised
    return nullptr;
  }

  jbyte result_flags[1];
  if (!exists) {
    result_flags[0] = 0;
  } else if (!value_found) {
    result_flags[0] = 1;
  } else {
    // found
    result_flags[0] = 2;
  }

  jobjectArray jresults = CABINDB_NAMESPACE::ByteJni::new2dByteArray(env, 2);
  if (jresults == nullptr) {
    // exception occurred
    return nullptr;
  }

  // prepare the result flag
  jbyteArray jresult_flags = env->NewByteArray(1);
  if (jresult_flags == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }
  env->SetByteArrayRegion(jresult_flags, 0, 1, result_flags);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(jresult_flags);
    return nullptr;
  }
  
  env->SetObjectArrayElement(jresults, 0, jresult_flags);
  if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->DeleteLocalRef(jresult_flags);
      return nullptr;
  }

  env->DeleteLocalRef(jresult_flags);

  if (result_flags[0] == 2) {
    // set the value
    const jsize jvalue_len = static_cast<jsize>(value.size());
    jbyteArray jresult_value = env->NewByteArray(jvalue_len);
    if (jresult_value == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }
    env->SetByteArrayRegion(jresult_value, 0, jvalue_len, 
        const_cast<jbyte*>(reinterpret_cast<const jbyte*>(value.data())));
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->DeleteLocalRef(jresult_value);
      return nullptr;
    }
    env->SetObjectArrayElement(jresults, 1, jresult_value);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->DeleteLocalRef(jresult_value);
      return nullptr;
    }

    env->DeleteLocalRef(jresult_value);
  }

  return jresults;
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    iterator
 * Signature: (J)J
 */
jlong Java_org_cabindb_CabinDB_iterator__J(
      JNIEnv*, jobject, jlong db_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(db_handle);
  return cabindb_iterator_helper(db, CABINDB_NAMESPACE::ReadOptions(), nullptr);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    iterator
 * Signature: (JJ)J
 */
jlong Java_org_cabindb_CabinDB_iterator__JJ(
    JNIEnv*, jobject, jlong db_handle, jlong jread_options_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(db_handle);
  auto& read_options =
      *reinterpret_cast<CABINDB_NAMESPACE::ReadOptions*>(jread_options_handle);
  return cabindb_iterator_helper(db, read_options, nullptr);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    iteratorCF
 * Signature: (JJ)J
 */
jlong Java_org_cabindb_CabinDB_iteratorCF__JJ(
    JNIEnv*, jobject, jlong db_handle, jlong jcf_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(db_handle);
  auto* cf_handle =
      reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  return cabindb_iterator_helper(db, CABINDB_NAMESPACE::ReadOptions(),
                                 cf_handle);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    iteratorCF
 * Signature: (JJJ)J
 */
jlong Java_org_cabindb_CabinDB_iteratorCF__JJJ(
    JNIEnv*, jobject,
    jlong db_handle, jlong jcf_handle, jlong jread_options_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(db_handle);
  auto* cf_handle =
      reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  auto& read_options =
      *reinterpret_cast<CABINDB_NAMESPACE::ReadOptions*>(jread_options_handle);
  return cabindb_iterator_helper(db, read_options, cf_handle);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    iterators
 * Signature: (J[JJ)[J
 */
jlongArray Java_org_cabindb_CabinDB_iterators(
    JNIEnv* env, jobject, jlong db_handle,
    jlongArray jcolumn_family_handles,
    jlong jread_options_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(db_handle);
  auto& read_options =
      *reinterpret_cast<CABINDB_NAMESPACE::ReadOptions*>(jread_options_handle);

  std::vector<CABINDB_NAMESPACE::ColumnFamilyHandle*> cf_handles;
  if (jcolumn_family_handles != nullptr) {
    const jsize len_cols = env->GetArrayLength(jcolumn_family_handles);
    jlong* jcfh = env->GetLongArrayElements(jcolumn_family_handles, nullptr);
    if (jcfh == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    for (jsize i = 0; i < len_cols; i++) {
      auto* cf_handle =
          reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcfh[i]);
      cf_handles.push_back(cf_handle);
    }

    env->ReleaseLongArrayElements(jcolumn_family_handles, jcfh, JNI_ABORT);
  }

  std::vector<CABINDB_NAMESPACE::Iterator*> iterators;
  CABINDB_NAMESPACE::Status s =
      db->NewIterators(read_options, cf_handles, &iterators);
  if (s.ok()) {
    jlongArray jLongArray =
        env->NewLongArray(static_cast<jsize>(iterators.size()));
    if (jLongArray == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    for (std::vector<CABINDB_NAMESPACE::Iterator*>::size_type i = 0;
         i < iterators.size(); i++) {
      env->SetLongArrayRegion(
          jLongArray, static_cast<jsize>(i), 1,
          const_cast<jlong*>(reinterpret_cast<const jlong*>(&iterators[i])));
      if (env->ExceptionCheck()) {
        // exception thrown: ArrayIndexOutOfBoundsException
        env->DeleteLocalRef(jLongArray);
        return nullptr;
      }
    }

    return jLongArray;
  } else {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }
}

/*
 * Method:    getSnapshot
 * Signature: (J)J
 */
jlong Java_org_cabindb_CabinDB_getSnapshot(
    JNIEnv*, jobject, jlong db_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(db_handle);
  const CABINDB_NAMESPACE::Snapshot* snapshot = db->GetSnapshot();
  return reinterpret_cast<jlong>(snapshot);
}

/*
 * Method:    releaseSnapshot
 * Signature: (JJ)V
 */
void Java_org_cabindb_CabinDB_releaseSnapshot(
    JNIEnv*, jobject, jlong db_handle,
    jlong snapshot_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(db_handle);
  auto* snapshot =
      reinterpret_cast<CABINDB_NAMESPACE::Snapshot*>(snapshot_handle);
  db->ReleaseSnapshot(snapshot);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    getProperty
 * Signature: (JJLjava/lang/String;I)Ljava/lang/String;
 */
jstring Java_org_cabindb_CabinDB_getProperty(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jcf_handle,
    jstring jproperty, jint jproperty_len) {
  const char* property = env->GetStringUTFChars(jproperty, nullptr);
  if (property == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }
  CABINDB_NAMESPACE::Slice property_name(property, jproperty_len);

  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  CABINDB_NAMESPACE::ColumnFamilyHandle* cf_handle;
  if (jcf_handle == 0) {
    cf_handle = db->DefaultColumnFamily();
  } else {
    cf_handle =
        reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  }

  std::string property_value;
  bool retCode = db->GetProperty(cf_handle, property_name, &property_value);
  env->ReleaseStringUTFChars(jproperty, property);

  if (retCode) {
    return env->NewStringUTF(property_value.c_str());
  }

  CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(
      env, CABINDB_NAMESPACE::Status::NotFound());
  return nullptr;
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    getMapProperty
 * Signature: (JJLjava/lang/String;I)Ljava/util/Map;
 */
jobject Java_org_cabindb_CabinDB_getMapProperty(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jcf_handle,
    jstring jproperty, jint jproperty_len) {
    const char* property = env->GetStringUTFChars(jproperty, nullptr);
  if (property == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }
  CABINDB_NAMESPACE::Slice property_name(property, jproperty_len);

  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  CABINDB_NAMESPACE::ColumnFamilyHandle* cf_handle;
  if (jcf_handle == 0) {
    cf_handle = db->DefaultColumnFamily();
  } else {
    cf_handle =
        reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  }

  std::map<std::string, std::string> property_value;
  bool retCode = db->GetMapProperty(cf_handle, property_name, &property_value);
  env->ReleaseStringUTFChars(jproperty, property);

  if (retCode) {
    return CABINDB_NAMESPACE::HashMapJni::fromCppMap(env, &property_value);
  }

  CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(
      env, CABINDB_NAMESPACE::Status::NotFound());
  return nullptr;
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    getLongProperty
 * Signature: (JJLjava/lang/String;I)J
 */
jlong Java_org_cabindb_CabinDB_getLongProperty(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jcf_handle,
    jstring jproperty, jint jproperty_len) {
  const char* property = env->GetStringUTFChars(jproperty, nullptr);
  if (property == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }
  CABINDB_NAMESPACE::Slice property_name(property, jproperty_len);

  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  CABINDB_NAMESPACE::ColumnFamilyHandle* cf_handle;
  if (jcf_handle == 0) {
    cf_handle = db->DefaultColumnFamily();
  } else {
    cf_handle =
        reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  }

  uint64_t property_value;
  bool retCode = db->GetIntProperty(cf_handle, property_name, &property_value);
  env->ReleaseStringUTFChars(jproperty, property);

  if (retCode) {
    return property_value;
  }

  CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(
      env, CABINDB_NAMESPACE::Status::NotFound());
  return 0;
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    resetStats
 * Signature: (J)V
 */
void Java_org_cabindb_CabinDB_resetStats(
    JNIEnv *, jobject, jlong jdb_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  db->ResetStats();
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    getAggregatedLongProperty
 * Signature: (JLjava/lang/String;I)J
 */
jlong Java_org_cabindb_CabinDB_getAggregatedLongProperty(
    JNIEnv* env, jobject, jlong db_handle,
    jstring jproperty, jint jproperty_len) {
  const char* property = env->GetStringUTFChars(jproperty, nullptr);
  if (property == nullptr) {
    return 0;
  }
  CABINDB_NAMESPACE::Slice property_name(property, jproperty_len);
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(db_handle);
  uint64_t property_value = 0;
  bool retCode = db->GetAggregatedIntProperty(property_name, &property_value);
  env->ReleaseStringUTFChars(jproperty, property);

  if (retCode) {
    return property_value;
  }

  CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(
      env, CABINDB_NAMESPACE::Status::NotFound());
  return 0;
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    getApproximateSizes
 * Signature: (JJ[JB)[J
 */
jlongArray Java_org_cabindb_CabinDB_getApproximateSizes(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jcf_handle,
    jlongArray jrange_slice_handles, jbyte jinclude_flags) {
  const jsize jlen = env->GetArrayLength(jrange_slice_handles);
  const size_t range_count = jlen / 2;

  jlong* jranges = env->GetLongArrayElements(jrange_slice_handles, nullptr);
  if (jranges == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  auto ranges = std::unique_ptr<CABINDB_NAMESPACE::Range[]>(
      new CABINDB_NAMESPACE::Range[range_count]);
  size_t range_offset = 0;
  for (jsize i = 0; i < jlen; ++i) {
    auto* start = reinterpret_cast<CABINDB_NAMESPACE::Slice*>(jranges[i]);
    auto* limit = reinterpret_cast<CABINDB_NAMESPACE::Slice*>(jranges[++i]);
    ranges.get()[range_offset++] = CABINDB_NAMESPACE::Range(*start, *limit);
  }

  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  CABINDB_NAMESPACE::ColumnFamilyHandle* cf_handle;
  if (jcf_handle == 0) {
    cf_handle = db->DefaultColumnFamily();
  } else {
    cf_handle =
        reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  }

  auto sizes = std::unique_ptr<uint64_t[]>(new uint64_t[range_count]);
  db->GetApproximateSizes(cf_handle, ranges.get(),
      static_cast<int>(range_count), sizes.get(),
      static_cast<uint8_t>(jinclude_flags));

  // release LongArrayElements
  env->ReleaseLongArrayElements(jrange_slice_handles, jranges, JNI_ABORT);

  // prepare results
  auto results = std::unique_ptr<jlong[]>(new jlong[range_count]);
  for (size_t i = 0; i < range_count; ++i) {
    results.get()[i] = static_cast<jlong>(sizes.get()[i]);
  }

  const jsize jrange_count = jlen / 2;
  jlongArray jresults = env->NewLongArray(jrange_count);
  if (jresults == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  env->SetLongArrayRegion(jresults, 0, jrange_count, results.get());
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(jresults);
    return nullptr;
  }

  return jresults;
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    getApproximateMemTableStats
 * Signature: (JJJJ)[J
 */
jlongArray Java_org_cabindb_CabinDB_getApproximateMemTableStats(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jcf_handle,
    jlong jstartHandle, jlong jlimitHandle) {
  auto* start = reinterpret_cast<CABINDB_NAMESPACE::Slice*>(jstartHandle);
  auto* limit = reinterpret_cast<CABINDB_NAMESPACE::Slice*>(jlimitHandle);
  const CABINDB_NAMESPACE::Range range(*start, *limit);

  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  CABINDB_NAMESPACE::ColumnFamilyHandle* cf_handle;
  if (jcf_handle == 0) {
    cf_handle = db->DefaultColumnFamily();
  } else {
    cf_handle =
        reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  }

  uint64_t count = 0;
  uint64_t sizes = 0;
  db->GetApproximateMemTableStats(cf_handle, range, &count, &sizes);

  // prepare results
  jlong results[2] = {
      static_cast<jlong>(count),
      static_cast<jlong>(sizes)};

  const jsize jcount = static_cast<jsize>(count);
  jlongArray jsizes = env->NewLongArray(jcount);
  if (jsizes == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  env->SetLongArrayRegion(jsizes, 0, jcount, results);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(jsizes);
    return nullptr;
  }

  return jsizes;
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    compactRange
 * Signature: (J[BI[BIJJ)V
 */
void Java_org_cabindb_CabinDB_compactRange(
    JNIEnv* env, jobject, jlong jdb_handle,
    jbyteArray jbegin, jint jbegin_len,
    jbyteArray jend, jint jend_len,
    jlong jcompact_range_opts_handle,
    jlong jcf_handle) {
  jboolean has_exception = JNI_FALSE;

  std::string str_begin;
  if (jbegin_len > 0) {
    str_begin = CABINDB_NAMESPACE::JniUtil::byteString<std::string>(
        env, jbegin, jbegin_len,
        [](const char* str, const size_t len) { return std::string(str, len); },
        &has_exception);
    if (has_exception == JNI_TRUE) {
      // exception occurred
      return;
    }
  }

  std::string str_end;
  if (jend_len > 0) {
    str_end = CABINDB_NAMESPACE::JniUtil::byteString<std::string>(
        env, jend, jend_len,
        [](const char* str, const size_t len) { return std::string(str, len); },
        &has_exception);
    if (has_exception == JNI_TRUE) {
      // exception occurred
      return;
    }
  }

  CABINDB_NAMESPACE::CompactRangeOptions* compact_range_opts = nullptr;
  if (jcompact_range_opts_handle == 0) {
    // NOTE: we DO own the pointer!
    compact_range_opts = new CABINDB_NAMESPACE::CompactRangeOptions();
  } else {
    // NOTE: we do NOT own the pointer!
    compact_range_opts =
        reinterpret_cast<CABINDB_NAMESPACE::CompactRangeOptions*>(
            jcompact_range_opts_handle);
  }

  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);

  CABINDB_NAMESPACE::ColumnFamilyHandle* cf_handle;
  if (jcf_handle == 0) {
    cf_handle = db->DefaultColumnFamily();
  } else {
    cf_handle =
        reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  }

  CABINDB_NAMESPACE::Status s;
  if (jbegin_len > 0 || jend_len > 0) {
    const CABINDB_NAMESPACE::Slice begin(str_begin);
    const CABINDB_NAMESPACE::Slice end(str_end);
    s = db->CompactRange(*compact_range_opts, cf_handle, &begin, &end);
  } else {
    s = db->CompactRange(*compact_range_opts, cf_handle, nullptr, nullptr);
  }

  if (jcompact_range_opts_handle == 0) {
    delete compact_range_opts;
  }
  CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    setOptions
 * Signature: (JJ[Ljava/lang/String;[Ljava/lang/String;)V
 */
void Java_org_cabindb_CabinDB_setOptions(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jcf_handle,
    jobjectArray jkeys, jobjectArray jvalues) {
  const jsize len = env->GetArrayLength(jkeys);
  assert(len == env->GetArrayLength(jvalues));

  std::unordered_map<std::string, std::string> options_map;
  for (jsize i = 0; i < len; i++) {
    jobject jobj_key = env->GetObjectArrayElement(jkeys, i);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      return;
    }

    jobject jobj_value = env->GetObjectArrayElement(jvalues, i);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->DeleteLocalRef(jobj_key);
      return;
    }

    jboolean has_exception = JNI_FALSE;
    std::string s_key = CABINDB_NAMESPACE::JniUtil::copyStdString(
        env, reinterpret_cast<jstring>(jobj_key), &has_exception);
    if (has_exception == JNI_TRUE) {
      // exception occurred
      env->DeleteLocalRef(jobj_value);
      env->DeleteLocalRef(jobj_key);
      return;
    }

    std::string s_value = CABINDB_NAMESPACE::JniUtil::copyStdString(
        env, reinterpret_cast<jstring>(jobj_value), &has_exception);
    if (has_exception == JNI_TRUE) {
      // exception occurred
      env->DeleteLocalRef(jobj_value);
      env->DeleteLocalRef(jobj_key);
      return;
    }

    options_map[s_key] = s_value;

    env->DeleteLocalRef(jobj_key);
    env->DeleteLocalRef(jobj_value);
  }

  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto* cf_handle =
      reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  auto s = db->SetOptions(cf_handle, options_map);
  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    setDBOptions
 * Signature: (J[Ljava/lang/String;[Ljava/lang/String;)V
 */
void Java_org_cabindb_CabinDB_setDBOptions(
    JNIEnv* env, jobject, jlong jdb_handle,
    jobjectArray jkeys, jobjectArray jvalues) {
  const jsize len = env->GetArrayLength(jkeys);
  assert(len == env->GetArrayLength(jvalues));

  std::unordered_map<std::string, std::string> options_map;
    for (jsize i = 0; i < len; i++) {
    jobject jobj_key = env->GetObjectArrayElement(jkeys, i);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      return;
    }

    jobject jobj_value = env->GetObjectArrayElement(jvalues, i);
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->DeleteLocalRef(jobj_key);
      return;
    }

    jboolean has_exception = JNI_FALSE;
    std::string s_key = CABINDB_NAMESPACE::JniUtil::copyStdString(
        env, reinterpret_cast<jstring>(jobj_key), &has_exception);
    if (has_exception == JNI_TRUE) {
      // exception occurred
      env->DeleteLocalRef(jobj_value);
      env->DeleteLocalRef(jobj_key);
      return;
    }

    std::string s_value = CABINDB_NAMESPACE::JniUtil::copyStdString(
        env, reinterpret_cast<jstring>(jobj_value), &has_exception);
    if (has_exception == JNI_TRUE) {
      // exception occurred
      env->DeleteLocalRef(jobj_value);
      env->DeleteLocalRef(jobj_key);
      return;
    }

    options_map[s_key] = s_value;

    env->DeleteLocalRef(jobj_key);
    env->DeleteLocalRef(jobj_value);
  }

  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto s = db->SetDBOptions(options_map);
  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    compactFiles
 * Signature: (JJJ[Ljava/lang/String;IIJ)[Ljava/lang/String;
 */
jobjectArray Java_org_cabindb_CabinDB_compactFiles(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jcompaction_opts_handle,
    jlong jcf_handle, jobjectArray jinput_file_names, jint joutput_level,
    jint joutput_path_id, jlong jcompaction_job_info_handle) {
  jboolean has_exception = JNI_FALSE;
  const std::vector<std::string> input_file_names =
      CABINDB_NAMESPACE::JniUtil::copyStrings(env, jinput_file_names,
                                              &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return nullptr;
  }

  auto* compaction_opts =
      reinterpret_cast<CABINDB_NAMESPACE::CompactionOptions*>(
          jcompaction_opts_handle);
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  CABINDB_NAMESPACE::ColumnFamilyHandle* cf_handle;
  if (jcf_handle == 0) {
    cf_handle = db->DefaultColumnFamily();
  } else {
    cf_handle =
        reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  }

  CABINDB_NAMESPACE::CompactionJobInfo* compaction_job_info = nullptr;
  if (jcompaction_job_info_handle != 0) {
    compaction_job_info =
        reinterpret_cast<CABINDB_NAMESPACE::CompactionJobInfo*>(
            jcompaction_job_info_handle);
  }

  std::vector<std::string> output_file_names;
  auto s = db->CompactFiles(*compaction_opts, cf_handle, input_file_names,
      static_cast<int>(joutput_level), static_cast<int>(joutput_path_id),
      &output_file_names, compaction_job_info);
  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }

  return CABINDB_NAMESPACE::JniUtil::toJavaStrings(env, &output_file_names);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    cancelAllBackgroundWork
 * Signature: (JZ)V
 */
void Java_org_cabindb_CabinDB_cancelAllBackgroundWork(
        JNIEnv*, jobject, jlong jdb_handle, jboolean jwait) {
    auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
    cabindb::CancelAllBackgroundWork(db, jwait);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    pauseBackgroundWork
 * Signature: (J)V
 */
void Java_org_cabindb_CabinDB_pauseBackgroundWork(
    JNIEnv* env, jobject, jlong jdb_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto s = db->PauseBackgroundWork();
  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    continueBackgroundWork
 * Signature: (J)V
 */
void Java_org_cabindb_CabinDB_continueBackgroundWork(
    JNIEnv* env, jobject, jlong jdb_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto s = db->ContinueBackgroundWork();
  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    enableAutoCompaction
 * Signature: (J[J)V
 */
void Java_org_cabindb_CabinDB_enableAutoCompaction(
    JNIEnv* env, jobject, jlong jdb_handle, jlongArray jcf_handles) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  jboolean has_exception = JNI_FALSE;
  const std::vector<CABINDB_NAMESPACE::ColumnFamilyHandle*> cf_handles =
      CABINDB_NAMESPACE::JniUtil::fromJPointers<
          CABINDB_NAMESPACE::ColumnFamilyHandle>(env, jcf_handles,
                                                 &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return;
  }
  db->EnableAutoCompaction(cf_handles);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    numberLevels
 * Signature: (JJ)I
 */
jint Java_org_cabindb_CabinDB_numberLevels(
    JNIEnv*, jobject, jlong jdb_handle, jlong jcf_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  CABINDB_NAMESPACE::ColumnFamilyHandle* cf_handle;
  if (jcf_handle == 0) {
    cf_handle = db->DefaultColumnFamily();
  } else {
    cf_handle =
        reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  }
  return static_cast<jint>(db->NumberLevels(cf_handle));
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    maxMemCompactionLevel
 * Signature: (JJ)I
 */
jint Java_org_cabindb_CabinDB_maxMemCompactionLevel(
    JNIEnv*, jobject, jlong jdb_handle, jlong jcf_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  CABINDB_NAMESPACE::ColumnFamilyHandle* cf_handle;
  if (jcf_handle == 0) {
    cf_handle = db->DefaultColumnFamily();
  } else {
    cf_handle =
        reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  }
  return static_cast<jint>(db->MaxMemCompactionLevel(cf_handle));
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    level0StopWriteTrigger
 * Signature: (JJ)I
 */
jint Java_org_cabindb_CabinDB_level0StopWriteTrigger(
    JNIEnv*, jobject, jlong jdb_handle, jlong jcf_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  CABINDB_NAMESPACE::ColumnFamilyHandle* cf_handle;
  if (jcf_handle == 0) {
    cf_handle = db->DefaultColumnFamily();
  } else {
    cf_handle =
        reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  }
  return static_cast<jint>(db->Level0StopWriteTrigger(cf_handle));
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    getName
 * Signature: (J)Ljava/lang/String;
 */
jstring Java_org_cabindb_CabinDB_getName(
    JNIEnv* env, jobject, jlong jdb_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  std::string name = db->GetName();
  return CABINDB_NAMESPACE::JniUtil::toJavaString(env, &name, false);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    getEnv
 * Signature: (J)J
 */
jlong Java_org_cabindb_CabinDB_getEnv(
    JNIEnv*, jobject, jlong jdb_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  return reinterpret_cast<jlong>(db->GetEnv());
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    flush
 * Signature: (JJ[J)V
 */
void Java_org_cabindb_CabinDB_flush(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jflush_opts_handle,
    jlongArray jcf_handles) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto* flush_opts =
      reinterpret_cast<CABINDB_NAMESPACE::FlushOptions*>(jflush_opts_handle);
  std::vector<CABINDB_NAMESPACE::ColumnFamilyHandle*> cf_handles;
  if (jcf_handles == nullptr) {
      cf_handles.push_back(db->DefaultColumnFamily());
  } else {
      jboolean has_exception = JNI_FALSE;
      cf_handles = CABINDB_NAMESPACE::JniUtil::fromJPointers<
          CABINDB_NAMESPACE::ColumnFamilyHandle>(env, jcf_handles,
                                                 &has_exception);
      if (has_exception) {
        // exception occurred
        return;
      }
  }
  auto s = db->Flush(*flush_opts, cf_handles);
  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    flushWal
 * Signature: (JZ)V
 */
void Java_org_cabindb_CabinDB_flushWal(
    JNIEnv* env, jobject, jlong jdb_handle, jboolean jsync) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto s = db->FlushWAL(jsync == JNI_TRUE);
  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    syncWal
 * Signature: (J)V
 */
void Java_org_cabindb_CabinDB_syncWal(
    JNIEnv* env, jobject, jlong jdb_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto s = db->SyncWAL();
  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    getLatestSequenceNumber
 * Signature: (J)V
 */
jlong Java_org_cabindb_CabinDB_getLatestSequenceNumber(
    JNIEnv*, jobject, jlong jdb_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  return db->GetLatestSequenceNumber();
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    setPreserveDeletesSequenceNumber
 * Signature: (JJ)Z
 */
jboolean Java_org_cabindb_CabinDB_setPreserveDeletesSequenceNumber(
    JNIEnv*, jobject, jlong jdb_handle, jlong jseq_number) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  if (db->SetPreserveDeletesSequenceNumber(
      static_cast<uint64_t>(jseq_number))) {
    return JNI_TRUE;
  } else {
    return JNI_FALSE;
  }
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    disableFileDeletions
 * Signature: (J)V
 */
void Java_org_cabindb_CabinDB_disableFileDeletions(
    JNIEnv* env, jobject, jlong jdb_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  CABINDB_NAMESPACE::Status s = db->DisableFileDeletions();
  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    enableFileDeletions
 * Signature: (JZ)V
 */
void Java_org_cabindb_CabinDB_enableFileDeletions(
    JNIEnv* env, jobject, jlong jdb_handle, jboolean jforce) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  CABINDB_NAMESPACE::Status s = db->EnableFileDeletions(jforce);
  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    getLiveFiles
 * Signature: (JZ)[Ljava/lang/String;
 */
jobjectArray Java_org_cabindb_CabinDB_getLiveFiles(
    JNIEnv* env, jobject, jlong jdb_handle, jboolean jflush_memtable) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  std::vector<std::string> live_files;
  uint64_t manifest_file_size = 0;
  auto s = db->GetLiveFiles(
      live_files, &manifest_file_size, jflush_memtable == JNI_TRUE);
  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }

  // append the manifest_file_size to the vector
  // for passing back to java
  live_files.push_back(std::to_string(manifest_file_size));

  return CABINDB_NAMESPACE::JniUtil::toJavaStrings(env, &live_files);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    getSortedWalFiles
 * Signature: (J)[Lorg/cabindb/LogFile;
 */
jobjectArray Java_org_cabindb_CabinDB_getSortedWalFiles(
    JNIEnv* env, jobject, jlong jdb_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  std::vector<std::unique_ptr<CABINDB_NAMESPACE::LogFile>> sorted_wal_files;
  auto s = db->GetSortedWalFiles(sorted_wal_files);
  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }

  // convert to Java type
  const jsize jlen = static_cast<jsize>(sorted_wal_files.size());
  jobjectArray jsorted_wal_files = env->NewObjectArray(
      jlen, CABINDB_NAMESPACE::LogFileJni::getJClass(env), nullptr);
  if(jsorted_wal_files == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  jsize i = 0;
  for (auto it = sorted_wal_files.begin(); it != sorted_wal_files.end(); ++it) {
    jobject jlog_file =
        CABINDB_NAMESPACE::LogFileJni::fromCppLogFile(env, it->get());
    if (jlog_file == nullptr) {
      // exception occurred
      env->DeleteLocalRef(jsorted_wal_files);
      return nullptr;
    }

    env->SetObjectArrayElement(jsorted_wal_files, i++, jlog_file);
    if (env->ExceptionCheck()) {
      // exception occurred
      env->DeleteLocalRef(jlog_file);
      env->DeleteLocalRef(jsorted_wal_files);
      return nullptr;
    }

    env->DeleteLocalRef(jlog_file);
  }

  return jsorted_wal_files;
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    getUpdatesSince
 * Signature: (JJ)J
 */
jlong Java_org_cabindb_CabinDB_getUpdatesSince(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jsequence_number) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  CABINDB_NAMESPACE::SequenceNumber sequence_number =
      static_cast<CABINDB_NAMESPACE::SequenceNumber>(jsequence_number);
  std::unique_ptr<CABINDB_NAMESPACE::TransactionLogIterator> iter;
  CABINDB_NAMESPACE::Status s = db->GetUpdatesSince(sequence_number, &iter);
  if (s.ok()) {
    return reinterpret_cast<jlong>(iter.release());
  }

  CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  return 0;
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    deleteFile
 * Signature: (JLjava/lang/String;)V
 */
void Java_org_cabindb_CabinDB_deleteFile(
    JNIEnv* env, jobject, jlong jdb_handle, jstring jname) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  jboolean has_exception = JNI_FALSE;
  std::string name =
      CABINDB_NAMESPACE::JniUtil::copyStdString(env, jname, &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return;
  }
  db->DeleteFile(name);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    getLiveFilesMetaData
 * Signature: (J)[Lorg/cabindb/LiveFileMetaData;
 */
jobjectArray Java_org_cabindb_CabinDB_getLiveFilesMetaData(
    JNIEnv* env, jobject, jlong jdb_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  std::vector<CABINDB_NAMESPACE::LiveFileMetaData> live_files_meta_data;
  db->GetLiveFilesMetaData(&live_files_meta_data);
  
  // convert to Java type
  const jsize jlen = static_cast<jsize>(live_files_meta_data.size());
  jobjectArray jlive_files_meta_data = env->NewObjectArray(
      jlen, CABINDB_NAMESPACE::LiveFileMetaDataJni::getJClass(env), nullptr);
  if(jlive_files_meta_data == nullptr) {
    // exception thrown: OutOfMemoryError
    return nullptr;
  }

  jsize i = 0;
  for (auto it = live_files_meta_data.begin(); it != live_files_meta_data.end(); ++it) {
    jobject jlive_file_meta_data =
        CABINDB_NAMESPACE::LiveFileMetaDataJni::fromCppLiveFileMetaData(env,
                                                                        &(*it));
    if (jlive_file_meta_data == nullptr) {
      // exception occurred
      env->DeleteLocalRef(jlive_files_meta_data);
      return nullptr;
    }

    env->SetObjectArrayElement(jlive_files_meta_data, i++, jlive_file_meta_data);
    if (env->ExceptionCheck()) {
      // exception occurred
      env->DeleteLocalRef(jlive_file_meta_data);
      env->DeleteLocalRef(jlive_files_meta_data);
      return nullptr;
    }

    env->DeleteLocalRef(jlive_file_meta_data);
  }

  return jlive_files_meta_data;
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    getColumnFamilyMetaData
 * Signature: (JJ)Lorg/cabindb/ColumnFamilyMetaData;
 */
jobject Java_org_cabindb_CabinDB_getColumnFamilyMetaData(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jcf_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  CABINDB_NAMESPACE::ColumnFamilyHandle* cf_handle;
  if (jcf_handle == 0) {
    cf_handle = db->DefaultColumnFamily();
  } else {
    cf_handle =
        reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  }
  CABINDB_NAMESPACE::ColumnFamilyMetaData cf_metadata;
  db->GetColumnFamilyMetaData(cf_handle, &cf_metadata);
  return CABINDB_NAMESPACE::ColumnFamilyMetaDataJni::
      fromCppColumnFamilyMetaData(env, &cf_metadata);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    ingestExternalFile
 * Signature: (JJ[Ljava/lang/String;IJ)V
 */
void Java_org_cabindb_CabinDB_ingestExternalFile(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jcf_handle,
    jobjectArray jfile_path_list, jint jfile_path_list_len,
    jlong jingest_external_file_options_handle) {
  jboolean has_exception = JNI_FALSE;
  std::vector<std::string> file_path_list =
      CABINDB_NAMESPACE::JniUtil::copyStrings(
          env, jfile_path_list, jfile_path_list_len, &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return;
  }

  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto* column_family =
      reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  auto* ifo = reinterpret_cast<CABINDB_NAMESPACE::IngestExternalFileOptions*>(
      jingest_external_file_options_handle);
  CABINDB_NAMESPACE::Status s =
      db->IngestExternalFile(column_family, file_path_list, *ifo);
  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    verifyChecksum
 * Signature: (J)V
 */
void Java_org_cabindb_CabinDB_verifyChecksum(
    JNIEnv* env, jobject, jlong jdb_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto s = db->VerifyChecksum();
  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    getDefaultColumnFamily
 * Signature: (J)J
 */
jlong Java_org_cabindb_CabinDB_getDefaultColumnFamily(
    JNIEnv*, jobject, jlong jdb_handle) {
  auto* db_handle = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto* cf_handle = db_handle->DefaultColumnFamily();
  return reinterpret_cast<jlong>(cf_handle);
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    getPropertiesOfAllTables
 * Signature: (JJ)Ljava/util/Map;
 */
jobject Java_org_cabindb_CabinDB_getPropertiesOfAllTables(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jcf_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  CABINDB_NAMESPACE::ColumnFamilyHandle* cf_handle;
  if (jcf_handle == 0) {
    cf_handle = db->DefaultColumnFamily();
  } else {
    cf_handle =
        reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  }
  CABINDB_NAMESPACE::TablePropertiesCollection table_properties_collection;
  auto s = db->GetPropertiesOfAllTables(cf_handle,
      &table_properties_collection);
  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  }
  
  // convert to Java type
  jobject jhash_map = CABINDB_NAMESPACE::HashMapJni::construct(
      env, static_cast<uint32_t>(table_properties_collection.size()));
  if (jhash_map == nullptr) {
    // exception occurred
    return nullptr;
  }

  const CABINDB_NAMESPACE::HashMapJni::FnMapKV<
      const std::string,
      const std::shared_ptr<const CABINDB_NAMESPACE::TableProperties>, jobject,
      jobject>
      fn_map_kv =
          [env](const std::pair<const std::string,
                                const std::shared_ptr<
                                    const CABINDB_NAMESPACE::TableProperties>>&
                    kv) {
            jstring jkey = CABINDB_NAMESPACE::JniUtil::toJavaString(
                env, &(kv.first), false);
            if (env->ExceptionCheck()) {
              // an error occurred
              return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
            }

            jobject jtable_properties =
                CABINDB_NAMESPACE::TablePropertiesJni::fromCppTableProperties(
                    env, *(kv.second.get()));
            if (jtable_properties == nullptr) {
              // an error occurred
              env->DeleteLocalRef(jkey);
              return std::unique_ptr<std::pair<jobject, jobject>>(nullptr);
            }

            return std::unique_ptr<std::pair<jobject, jobject>>(
                new std::pair<jobject, jobject>(
                    static_cast<jobject>(jkey),
                    static_cast<jobject>(jtable_properties)));
          };

  if (!CABINDB_NAMESPACE::HashMapJni::putAll(
          env, jhash_map, table_properties_collection.begin(),
          table_properties_collection.end(), fn_map_kv)) {
    // exception occurred
    return nullptr;
  }

  return jhash_map;
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    getPropertiesOfTablesInRange
 * Signature: (JJ[J)Ljava/util/Map;
 */
jobject Java_org_cabindb_CabinDB_getPropertiesOfTablesInRange(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jcf_handle,
    jlongArray jrange_slice_handles) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  CABINDB_NAMESPACE::ColumnFamilyHandle* cf_handle;
  if (jcf_handle == 0) {
    cf_handle = db->DefaultColumnFamily();
  } else {
    cf_handle =
        reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  }
  const jsize jlen = env->GetArrayLength(jrange_slice_handles);
  jlong* jrange_slice_handle =
      env->GetLongArrayElements(jrange_slice_handles, nullptr);
  if (jrange_slice_handle == nullptr) {
    // exception occurred
    return nullptr;
  }

  const size_t ranges_len = static_cast<size_t>(jlen / 2);
  auto ranges = std::unique_ptr<CABINDB_NAMESPACE::Range[]>(
      new CABINDB_NAMESPACE::Range[ranges_len]);
  for (jsize i = 0, j = 0; i < jlen; ++i) {
    auto* start =
        reinterpret_cast<CABINDB_NAMESPACE::Slice*>(jrange_slice_handle[i]);
    auto* limit =
        reinterpret_cast<CABINDB_NAMESPACE::Slice*>(jrange_slice_handle[++i]);
    ranges[j++] = CABINDB_NAMESPACE::Range(*start, *limit);
  }

  CABINDB_NAMESPACE::TablePropertiesCollection table_properties_collection;
  auto s = db->GetPropertiesOfTablesInRange(
      cf_handle, ranges.get(), ranges_len, &table_properties_collection);
  if (!s.ok()) {
    // error occurred
    env->ReleaseLongArrayElements(jrange_slice_handles, jrange_slice_handle, JNI_ABORT);
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }

  // cleanup
  env->ReleaseLongArrayElements(jrange_slice_handles, jrange_slice_handle, JNI_ABORT);

  return jrange_slice_handles;
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    suggestCompactRange
 * Signature: (JJ)[J
 */
jlongArray Java_org_cabindb_CabinDB_suggestCompactRange(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jcf_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  CABINDB_NAMESPACE::ColumnFamilyHandle* cf_handle;
  if (jcf_handle == 0) {
    cf_handle = db->DefaultColumnFamily();
  } else {
    cf_handle =
        reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  }
  auto* begin = new CABINDB_NAMESPACE::Slice();
  auto* end = new CABINDB_NAMESPACE::Slice();
  auto s = db->SuggestCompactRange(cf_handle, begin, end);
  if (!s.ok()) {
    // error occurred
    delete begin;
    delete end;
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }

  jlongArray jslice_handles = env->NewLongArray(2);
  if (jslice_handles == nullptr) {
    // exception thrown: OutOfMemoryError
    delete begin;
    delete end;
    return nullptr;
  }

  jlong slice_handles[2];
  slice_handles[0] = reinterpret_cast<jlong>(begin);
  slice_handles[1] = reinterpret_cast<jlong>(end);
  env->SetLongArrayRegion(jslice_handles, 0, 2, slice_handles);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete begin;
    delete end;
    env->DeleteLocalRef(jslice_handles);
    return nullptr;
  }

  return jslice_handles;
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    promoteL0
 * Signature: (JJI)V
 */
void Java_org_cabindb_CabinDB_promoteL0(
    JNIEnv*, jobject, jlong jdb_handle, jlong jcf_handle, jint jtarget_level) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  CABINDB_NAMESPACE::ColumnFamilyHandle* cf_handle;
  if (jcf_handle == 0) {
    cf_handle = db->DefaultColumnFamily();
  } else {
    cf_handle =
        reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  }
  db->PromoteL0(cf_handle, static_cast<int>(jtarget_level));
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    startTrace
 * Signature: (JJJ)V
 */
void Java_org_cabindb_CabinDB_startTrace(
    JNIEnv* env, jobject, jlong jdb_handle, jlong jmax_trace_file_size,
    jlong jtrace_writer_jnicallback_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  CABINDB_NAMESPACE::TraceOptions trace_options;
  trace_options.max_trace_file_size = 
      static_cast<uint64_t>(jmax_trace_file_size);
  // transfer ownership of trace writer from Java to C++
  auto trace_writer =
      std::unique_ptr<CABINDB_NAMESPACE::TraceWriterJniCallback>(
          reinterpret_cast<CABINDB_NAMESPACE::TraceWriterJniCallback*>(
              jtrace_writer_jnicallback_handle));
  auto s = db->StartTrace(trace_options, std::move(trace_writer));
  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    endTrace
 * Signature: (J)V
 */
void Java_org_cabindb_CabinDB_endTrace(JNIEnv* env, jobject, jlong jdb_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto s = db->EndTrace();
  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    tryCatchUpWithPrimary
 * Signature: (J)V
 */
void Java_org_cabindb_CabinDB_tryCatchUpWithPrimary(JNIEnv* env, jobject,
                                                    jlong jdb_handle) {
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto s = db->TryCatchUpWithPrimary();
  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    destroyDB
 * Signature: (Ljava/lang/String;J)V
 */
void Java_org_cabindb_CabinDB_destroyDB(
    JNIEnv* env, jclass, jstring jdb_path, jlong joptions_handle) {
  const char* db_path = env->GetStringUTFChars(jdb_path, nullptr);
  if (db_path == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  auto* options =
      reinterpret_cast<CABINDB_NAMESPACE::Options*>(joptions_handle);
  if (options == nullptr) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(
        env, CABINDB_NAMESPACE::Status::InvalidArgument("Invalid Options."));
  }

  CABINDB_NAMESPACE::Status s = CABINDB_NAMESPACE::DestroyDB(db_path, *options);
  env->ReleaseStringUTFChars(jdb_path, db_path);

  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  }
}

bool get_slice_helper(JNIEnv* env, jobjectArray ranges, jsize index,
                      std::unique_ptr<CABINDB_NAMESPACE::Slice>& slice,
                      std::vector<std::unique_ptr<jbyte[]>>& ranges_to_free) {
  jobject jArray = env->GetObjectArrayElement(ranges, index);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    return false;
  }

  if (jArray == nullptr) {
    return true;
  }

  jbyteArray jba = reinterpret_cast<jbyteArray>(jArray);
  jsize len_ba = env->GetArrayLength(jba);
  ranges_to_free.push_back(std::unique_ptr<jbyte[]>(new jbyte[len_ba]));
  env->GetByteArrayRegion(jba, 0, len_ba, ranges_to_free.back().get());
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(jArray);
    return false;
  }
  env->DeleteLocalRef(jArray);
  slice.reset(new CABINDB_NAMESPACE::Slice(
      reinterpret_cast<char*>(ranges_to_free.back().get()), len_ba));
  return true;
}
/*
 * Class:     org_cabindb_CabinDB
 * Method:    deleteFilesInRanges
 * Signature: (JJLjava/util/List;Z)V
 */
void Java_org_cabindb_CabinDB_deleteFilesInRanges(JNIEnv* env, jobject /*jdb*/,
                                                  jlong jdb_handle,
                                                  jlong jcf_handle,
                                                  jobjectArray ranges,
                                                  jboolean include_end) {
  jsize length = env->GetArrayLength(ranges);

  std::vector<CABINDB_NAMESPACE::RangePtr> rangesVector;
  std::vector<std::unique_ptr<CABINDB_NAMESPACE::Slice>> slices;
  std::vector<std::unique_ptr<jbyte[]>> ranges_to_free;
  for (jsize i = 0; (i + 1) < length; i += 2) {
    slices.push_back(std::unique_ptr<CABINDB_NAMESPACE::Slice>());
    if (!get_slice_helper(env, ranges, i, slices.back(), ranges_to_free)) {
      // exception thrown
      return;
    }

    slices.push_back(std::unique_ptr<CABINDB_NAMESPACE::Slice>());
    if (!get_slice_helper(env, ranges, i + 1, slices.back(), ranges_to_free)) {
      // exception thrown
      return;
    }

    rangesVector.push_back(CABINDB_NAMESPACE::RangePtr(
        slices[slices.size() - 2].get(), slices[slices.size() - 1].get()));
  }

  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto* column_family =
      reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);

  CABINDB_NAMESPACE::Status s = CABINDB_NAMESPACE::DeleteFilesInRanges(
      db, column_family == nullptr ? db->DefaultColumnFamily() : column_family,
      rangesVector.data(), rangesVector.size(), include_end);

  if (!s.ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_cabindb_CabinDB
 * Method:    version
 * Signature: ()I
 */
jint Java_org_cabindb_CabinDB_version(JNIEnv*, jclass) {
  uint32_t encodedVersion = (CABINDB_MAJOR & 0xff) << 16;
  encodedVersion |= (CABINDB_MINOR & 0xff) << 8;
  encodedVersion |= (CABINDB_PATCH & 0xff);
  return static_cast<jint>(encodedVersion);
}
