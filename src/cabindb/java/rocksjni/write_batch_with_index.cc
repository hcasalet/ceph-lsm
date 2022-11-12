// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ CABINDB_NAMESPACE::WriteBatchWithIndex methods from Java side.

#include "cabindb/utilities/write_batch_with_index.h"
#include "include/org_cabindb_WBWICabinIterator.h"
#include "include/org_cabindb_WriteBatchWithIndex.h"
#include "cabindb/comparator.h"
#include "cabinjni/portal.h"

/*
 * Class:     org_cabindb_WriteBatchWithIndex
 * Method:    newWriteBatchWithIndex
 * Signature: ()J
 */
jlong Java_org_cabindb_WriteBatchWithIndex_newWriteBatchWithIndex__(
    JNIEnv* /*env*/, jclass /*jcls*/) {
  auto* wbwi = new CABINDB_NAMESPACE::WriteBatchWithIndex();
  return reinterpret_cast<jlong>(wbwi);
}

/*
 * Class:     org_cabindb_WriteBatchWithIndex
 * Method:    newWriteBatchWithIndex
 * Signature: (Z)J
 */
jlong Java_org_cabindb_WriteBatchWithIndex_newWriteBatchWithIndex__Z(
    JNIEnv* /*env*/, jclass /*jcls*/, jboolean joverwrite_key) {
  auto* wbwi = new CABINDB_NAMESPACE::WriteBatchWithIndex(
      CABINDB_NAMESPACE::BytewiseComparator(), 0,
      static_cast<bool>(joverwrite_key));
  return reinterpret_cast<jlong>(wbwi);
}

/*
 * Class:     org_cabindb_WriteBatchWithIndex
 * Method:    newWriteBatchWithIndex
 * Signature: (JBIZ)J
 */
jlong Java_org_cabindb_WriteBatchWithIndex_newWriteBatchWithIndex__JBIZ(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jfallback_index_comparator_handle,
    jbyte jcomparator_type, jint jreserved_bytes, jboolean joverwrite_key) {
  CABINDB_NAMESPACE::Comparator* fallback_comparator = nullptr;
  switch (jcomparator_type) {
    // JAVA_COMPARATOR
    case 0x0:
      fallback_comparator =
          reinterpret_cast<CABINDB_NAMESPACE::ComparatorJniCallback*>(
              jfallback_index_comparator_handle);
      break;

    // JAVA_NATIVE_COMPARATOR_WRAPPER
    case 0x1:
      fallback_comparator = reinterpret_cast<CABINDB_NAMESPACE::Comparator*>(
          jfallback_index_comparator_handle);
      break;
  }
  auto* wbwi = new CABINDB_NAMESPACE::WriteBatchWithIndex(
      fallback_comparator, static_cast<size_t>(jreserved_bytes),
      static_cast<bool>(joverwrite_key));
  return reinterpret_cast<jlong>(wbwi);
}

/*
 * Class:     org_cabindb_WriteBatchWithIndex
 * Method:    count0
 * Signature: (J)I
 */
jint Java_org_cabindb_WriteBatchWithIndex_count0(JNIEnv* /*env*/,
                                                 jobject /*jobj*/,
                                                 jlong jwbwi_handle) {
  auto* wbwi =
      reinterpret_cast<CABINDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);

  return static_cast<jint>(wbwi->GetWriteBatch()->Count());
}

/*
 * Class:     org_cabindb_WriteBatchWithIndex
 * Method:    put
 * Signature: (J[BI[BI)V
 */
void Java_org_cabindb_WriteBatchWithIndex_put__J_3BI_3BI(
    JNIEnv* env, jobject jobj, jlong jwbwi_handle, jbyteArray jkey,
    jint jkey_len, jbyteArray jentry_value, jint jentry_value_len) {
  auto* wbwi =
      reinterpret_cast<CABINDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);
  auto put = [&wbwi](CABINDB_NAMESPACE::Slice key,
                     CABINDB_NAMESPACE::Slice value) {
    return wbwi->Put(key, value);
  };
  std::unique_ptr<CABINDB_NAMESPACE::Status> status =
      CABINDB_NAMESPACE::JniUtil::kv_op(put, env, jobj, jkey, jkey_len,
                                        jentry_value, jentry_value_len);
  if (status != nullptr && !status->ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_cabindb_WriteBatchWithIndex
 * Method:    put
 * Signature: (J[BI[BIJ)V
 */
void Java_org_cabindb_WriteBatchWithIndex_put__J_3BI_3BIJ(
    JNIEnv* env, jobject jobj, jlong jwbwi_handle, jbyteArray jkey,
    jint jkey_len, jbyteArray jentry_value, jint jentry_value_len,
    jlong jcf_handle) {
  auto* wbwi =
      reinterpret_cast<CABINDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);
  auto* cf_handle =
      reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  assert(cf_handle != nullptr);
  auto put = [&wbwi, &cf_handle](CABINDB_NAMESPACE::Slice key,
                                 CABINDB_NAMESPACE::Slice value) {
    return wbwi->Put(cf_handle, key, value);
  };
  std::unique_ptr<CABINDB_NAMESPACE::Status> status =
      CABINDB_NAMESPACE::JniUtil::kv_op(put, env, jobj, jkey, jkey_len,
                                        jentry_value, jentry_value_len);
  if (status != nullptr && !status->ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_cabindb_WriteBatchWithIndex
 * Method:    putDirect
 * Signature: (JLjava/nio/ByteBuffer;IILjava/nio/ByteBuffer;IIJ)V
 */
void Java_org_cabindb_WriteBatchWithIndex_putDirect(
    JNIEnv* env, jobject /*jobj*/, jlong jwb_handle, jobject jkey,
    jint jkey_offset, jint jkey_len, jobject jval, jint jval_offset,
    jint jval_len, jlong jcf_handle) {
  auto* wb = reinterpret_cast<CABINDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
  auto* cf_handle =
      reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  auto put = [&wb, &cf_handle](CABINDB_NAMESPACE::Slice& key,
                               CABINDB_NAMESPACE::Slice& value) {
    if (cf_handle == nullptr) {
      wb->Put(key, value);
    } else {
      wb->Put(cf_handle, key, value);
    }
  };
  CABINDB_NAMESPACE::JniUtil::kv_op_direct(
      put, env, jkey, jkey_offset, jkey_len, jval, jval_offset, jval_len);
}

/*
 * Class:     org_cabindb_WriteBatchWithIndex
 * Method:    merge
 * Signature: (J[BI[BI)V
 */
void Java_org_cabindb_WriteBatchWithIndex_merge__J_3BI_3BI(
    JNIEnv* env, jobject jobj, jlong jwbwi_handle, jbyteArray jkey,
    jint jkey_len, jbyteArray jentry_value, jint jentry_value_len) {
  auto* wbwi =
      reinterpret_cast<CABINDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);
  auto merge = [&wbwi](CABINDB_NAMESPACE::Slice key,
                       CABINDB_NAMESPACE::Slice value) {
    return wbwi->Merge(key, value);
  };
  std::unique_ptr<CABINDB_NAMESPACE::Status> status =
      CABINDB_NAMESPACE::JniUtil::kv_op(merge, env, jobj, jkey, jkey_len,
                                        jentry_value, jentry_value_len);
  if (status != nullptr && !status->ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_cabindb_WriteBatchWithIndex
 * Method:    merge
 * Signature: (J[BI[BIJ)V
 */
void Java_org_cabindb_WriteBatchWithIndex_merge__J_3BI_3BIJ(
    JNIEnv* env, jobject jobj, jlong jwbwi_handle, jbyteArray jkey,
    jint jkey_len, jbyteArray jentry_value, jint jentry_value_len,
    jlong jcf_handle) {
  auto* wbwi =
      reinterpret_cast<CABINDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);
  auto* cf_handle =
      reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  assert(cf_handle != nullptr);
  auto merge = [&wbwi, &cf_handle](CABINDB_NAMESPACE::Slice key,
                                   CABINDB_NAMESPACE::Slice value) {
    return wbwi->Merge(cf_handle, key, value);
  };
  std::unique_ptr<CABINDB_NAMESPACE::Status> status =
      CABINDB_NAMESPACE::JniUtil::kv_op(merge, env, jobj, jkey, jkey_len,
                                        jentry_value, jentry_value_len);
  if (status != nullptr && !status->ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_cabindb_WriteBatchWithIndex
 * Method:    delete
 * Signature: (J[BI)V
 */
void Java_org_cabindb_WriteBatchWithIndex_delete__J_3BI(JNIEnv* env,
                                                        jobject jobj,
                                                        jlong jwbwi_handle,
                                                        jbyteArray jkey,
                                                        jint jkey_len) {
  auto* wbwi =
      reinterpret_cast<CABINDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);
  auto remove = [&wbwi](CABINDB_NAMESPACE::Slice key) {
    return wbwi->Delete(key);
  };
  std::unique_ptr<CABINDB_NAMESPACE::Status> status =
      CABINDB_NAMESPACE::JniUtil::k_op(remove, env, jobj, jkey, jkey_len);
  if (status != nullptr && !status->ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_cabindb_WriteBatchWithIndex
 * Method:    delete
 * Signature: (J[BIJ)V
 */
void Java_org_cabindb_WriteBatchWithIndex_delete__J_3BIJ(
    JNIEnv* env, jobject jobj, jlong jwbwi_handle, jbyteArray jkey,
    jint jkey_len, jlong jcf_handle) {
  auto* wbwi =
      reinterpret_cast<CABINDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);
  auto* cf_handle =
      reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  assert(cf_handle != nullptr);
  auto remove = [&wbwi, &cf_handle](CABINDB_NAMESPACE::Slice key) {
    return wbwi->Delete(cf_handle, key);
  };
  std::unique_ptr<CABINDB_NAMESPACE::Status> status =
      CABINDB_NAMESPACE::JniUtil::k_op(remove, env, jobj, jkey, jkey_len);
  if (status != nullptr && !status->ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_cabindb_WriteBatchWithIndex
 * Method:    singleDelete
 * Signature: (J[BI)V
 */
void Java_org_cabindb_WriteBatchWithIndex_singleDelete__J_3BI(
    JNIEnv* env, jobject jobj, jlong jwbwi_handle, jbyteArray jkey,
    jint jkey_len) {
  auto* wbwi =
      reinterpret_cast<CABINDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);
  auto single_delete = [&wbwi](CABINDB_NAMESPACE::Slice key) {
    return wbwi->SingleDelete(key);
  };
  std::unique_ptr<CABINDB_NAMESPACE::Status> status =
      CABINDB_NAMESPACE::JniUtil::k_op(single_delete, env, jobj, jkey,
                                       jkey_len);
  if (status != nullptr && !status->ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_cabindb_WriteBatchWithIndex
 * Method:    singleDelete
 * Signature: (J[BIJ)V
 */
void Java_org_cabindb_WriteBatchWithIndex_singleDelete__J_3BIJ(
    JNIEnv* env, jobject jobj, jlong jwbwi_handle, jbyteArray jkey,
    jint jkey_len, jlong jcf_handle) {
  auto* wbwi =
      reinterpret_cast<CABINDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);
  auto* cf_handle =
      reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  assert(cf_handle != nullptr);
  auto single_delete = [&wbwi, &cf_handle](CABINDB_NAMESPACE::Slice key) {
    return wbwi->SingleDelete(cf_handle, key);
  };
  std::unique_ptr<CABINDB_NAMESPACE::Status> status =
      CABINDB_NAMESPACE::JniUtil::k_op(single_delete, env, jobj, jkey,
                                       jkey_len);
  if (status != nullptr && !status->ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_cabindb_WriteBatchWithIndex
 * Method:    removeDirect
 * Signature: (JLjava/nio/ByteBuffer;IIJ)V
 */
void Java_org_cabindb_WriteBatchWithIndex_removeDirect(
    JNIEnv* env, jobject /*jobj*/, jlong jwb_handle, jobject jkey,
    jint jkey_offset, jint jkey_len, jlong jcf_handle) {
  auto* wb = reinterpret_cast<CABINDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
  auto* cf_handle =
      reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  auto remove = [&wb, &cf_handle](CABINDB_NAMESPACE::Slice& key) {
    if (cf_handle == nullptr) {
      wb->Delete(key);
    } else {
      wb->Delete(cf_handle, key);
    }
  };
  CABINDB_NAMESPACE::JniUtil::k_op_direct(remove, env, jkey, jkey_offset,
                                          jkey_len);
}

/*
 * Class:     org_cabindb_WriteBatchWithIndex
 * Method:    deleteRange
 * Signature: (J[BI[BI)V
 */
void Java_org_cabindb_WriteBatchWithIndex_deleteRange__J_3BI_3BI(
    JNIEnv* env, jobject jobj, jlong jwbwi_handle, jbyteArray jbegin_key,
    jint jbegin_key_len, jbyteArray jend_key, jint jend_key_len) {
  auto* wbwi =
      reinterpret_cast<CABINDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);
  auto deleteRange = [&wbwi](CABINDB_NAMESPACE::Slice beginKey,
                             CABINDB_NAMESPACE::Slice endKey) {
    return wbwi->DeleteRange(beginKey, endKey);
  };
  std::unique_ptr<CABINDB_NAMESPACE::Status> status =
      CABINDB_NAMESPACE::JniUtil::kv_op(deleteRange, env, jobj, jbegin_key,
                                        jbegin_key_len, jend_key, jend_key_len);
  if (status != nullptr && !status->ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_cabindb_WriteBatchWithIndex
 * Method:    deleteRange
 * Signature: (J[BI[BIJ)V
 */
void Java_org_cabindb_WriteBatchWithIndex_deleteRange__J_3BI_3BIJ(
    JNIEnv* env, jobject jobj, jlong jwbwi_handle, jbyteArray jbegin_key,
    jint jbegin_key_len, jbyteArray jend_key, jint jend_key_len,
    jlong jcf_handle) {
  auto* wbwi =
      reinterpret_cast<CABINDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);
  auto* cf_handle =
      reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  assert(cf_handle != nullptr);
  auto deleteRange = [&wbwi, &cf_handle](CABINDB_NAMESPACE::Slice beginKey,
                                         CABINDB_NAMESPACE::Slice endKey) {
    return wbwi->DeleteRange(cf_handle, beginKey, endKey);
  };
  std::unique_ptr<CABINDB_NAMESPACE::Status> status =
      CABINDB_NAMESPACE::JniUtil::kv_op(deleteRange, env, jobj, jbegin_key,
                                        jbegin_key_len, jend_key, jend_key_len);
  if (status != nullptr && !status->ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_cabindb_WriteBatchWithIndex
 * Method:    putLogData
 * Signature: (J[BI)V
 */
void Java_org_cabindb_WriteBatchWithIndex_putLogData(JNIEnv* env, jobject jobj,
                                                     jlong jwbwi_handle,
                                                     jbyteArray jblob,
                                                     jint jblob_len) {
  auto* wbwi =
      reinterpret_cast<CABINDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);
  auto putLogData = [&wbwi](CABINDB_NAMESPACE::Slice blob) {
    return wbwi->PutLogData(blob);
  };
  std::unique_ptr<CABINDB_NAMESPACE::Status> status =
      CABINDB_NAMESPACE::JniUtil::k_op(putLogData, env, jobj, jblob, jblob_len);
  if (status != nullptr && !status->ok()) {
    CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_cabindb_WriteBatchWithIndex
 * Method:    clear
 * Signature: (J)V
 */
void Java_org_cabindb_WriteBatchWithIndex_clear0(JNIEnv* /*env*/,
                                                 jobject /*jobj*/,
                                                 jlong jwbwi_handle) {
  auto* wbwi =
      reinterpret_cast<CABINDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);

  wbwi->Clear();
}

/*
 * Class:     org_cabindb_WriteBatchWithIndex
 * Method:    setSavePoint0
 * Signature: (J)V
 */
void Java_org_cabindb_WriteBatchWithIndex_setSavePoint0(JNIEnv* /*env*/,
                                                        jobject /*jobj*/,
                                                        jlong jwbwi_handle) {
  auto* wbwi =
      reinterpret_cast<CABINDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);

  wbwi->SetSavePoint();
}

/*
 * Class:     org_cabindb_WriteBatchWithIndex
 * Method:    rollbackToSavePoint0
 * Signature: (J)V
 */
void Java_org_cabindb_WriteBatchWithIndex_rollbackToSavePoint0(
    JNIEnv* env, jobject /*jobj*/, jlong jwbwi_handle) {
  auto* wbwi =
      reinterpret_cast<CABINDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);

  auto s = wbwi->RollbackToSavePoint();

  if (s.ok()) {
    return;
  }

  CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_cabindb_WriteBatchWithIndex
 * Method:    popSavePoint
 * Signature: (J)V
 */
void Java_org_cabindb_WriteBatchWithIndex_popSavePoint(JNIEnv* env,
                                                       jobject /*jobj*/,
                                                       jlong jwbwi_handle) {
  auto* wbwi =
      reinterpret_cast<CABINDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);

  auto s = wbwi->PopSavePoint();

  if (s.ok()) {
    return;
  }

  CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_cabindb_WriteBatchWithIndex
 * Method:    setMaxBytes
 * Signature: (JJ)V
 */
void Java_org_cabindb_WriteBatchWithIndex_setMaxBytes(JNIEnv* /*env*/,
                                                      jobject /*jobj*/,
                                                      jlong jwbwi_handle,
                                                      jlong jmax_bytes) {
  auto* wbwi =
      reinterpret_cast<CABINDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);

  wbwi->SetMaxBytes(static_cast<size_t>(jmax_bytes));
}

/*
 * Class:     org_cabindb_WriteBatchWithIndex
 * Method:    getWriteBatch
 * Signature: (J)Lorg/cabindb/WriteBatch;
 */
jobject Java_org_cabindb_WriteBatchWithIndex_getWriteBatch(JNIEnv* env,
                                                           jobject /*jobj*/,
                                                           jlong jwbwi_handle) {
  auto* wbwi =
      reinterpret_cast<CABINDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  assert(wbwi != nullptr);

  auto* wb = wbwi->GetWriteBatch();

  // TODO(AR) is the `wb` object owned by us?
  return CABINDB_NAMESPACE::WriteBatchJni::construct(env, wb);
}

/*
 * Class:     org_cabindb_WriteBatchWithIndex
 * Method:    iterator0
 * Signature: (J)J
 */
jlong Java_org_cabindb_WriteBatchWithIndex_iterator0(JNIEnv* /*env*/,
                                                     jobject /*jobj*/,
                                                     jlong jwbwi_handle) {
  auto* wbwi =
      reinterpret_cast<CABINDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  auto* wbwi_iterator = wbwi->NewIterator();
  return reinterpret_cast<jlong>(wbwi_iterator);
}

/*
 * Class:     org_cabindb_WriteBatchWithIndex
 * Method:    iterator1
 * Signature: (JJ)J
 */
jlong Java_org_cabindb_WriteBatchWithIndex_iterator1(JNIEnv* /*env*/,
                                                     jobject /*jobj*/,
                                                     jlong jwbwi_handle,
                                                     jlong jcf_handle) {
  auto* wbwi =
      reinterpret_cast<CABINDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  auto* cf_handle =
      reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  auto* wbwi_iterator = wbwi->NewIterator(cf_handle);
  return reinterpret_cast<jlong>(wbwi_iterator);
}

/*
 * Class:     org_cabindb_WriteBatchWithIndex
 * Method:    iteratorWithBase
 * Signature: (JJJJ)J
 */
jlong Java_org_cabindb_WriteBatchWithIndex_iteratorWithBase(
    JNIEnv*, jobject, jlong jwbwi_handle, jlong jcf_handle,
    jlong jbase_iterator_handle, jlong jread_opts_handle) {
  auto* wbwi =
      reinterpret_cast<CABINDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  auto* cf_handle =
      reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  auto* base_iterator =
      reinterpret_cast<CABINDB_NAMESPACE::Iterator*>(jbase_iterator_handle);
  CABINDB_NAMESPACE::ReadOptions* read_opts =
      jread_opts_handle == 0
          ? nullptr
          : reinterpret_cast<CABINDB_NAMESPACE::ReadOptions*>(
                jread_opts_handle);
  auto* iterator =
      wbwi->NewIteratorWithBase(cf_handle, base_iterator, read_opts);
  return reinterpret_cast<jlong>(iterator);
}

/*
 * Class:     org_cabindb_WriteBatchWithIndex
 * Method:    getFromBatch
 * Signature: (JJ[BI)[B
 */
jbyteArray JNICALL Java_org_cabindb_WriteBatchWithIndex_getFromBatch__JJ_3BI(
    JNIEnv* env, jobject /*jobj*/, jlong jwbwi_handle, jlong jdbopt_handle,
    jbyteArray jkey, jint jkey_len) {
  auto* wbwi =
      reinterpret_cast<CABINDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  auto* dbopt = reinterpret_cast<CABINDB_NAMESPACE::DBOptions*>(jdbopt_handle);

  auto getter = [&wbwi, &dbopt](const CABINDB_NAMESPACE::Slice& key,
                                std::string* value) {
    return wbwi->GetFromBatch(*dbopt, key, value);
  };

  return CABINDB_NAMESPACE::JniUtil::v_op(getter, env, jkey, jkey_len);
}

/*
 * Class:     org_cabindb_WriteBatchWithIndex
 * Method:    getFromBatch
 * Signature: (JJ[BIJ)[B
 */
jbyteArray Java_org_cabindb_WriteBatchWithIndex_getFromBatch__JJ_3BIJ(
    JNIEnv* env, jobject /*jobj*/, jlong jwbwi_handle, jlong jdbopt_handle,
    jbyteArray jkey, jint jkey_len, jlong jcf_handle) {
  auto* wbwi =
      reinterpret_cast<CABINDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  auto* dbopt = reinterpret_cast<CABINDB_NAMESPACE::DBOptions*>(jdbopt_handle);
  auto* cf_handle =
      reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);

  auto getter = [&wbwi, &cf_handle, &dbopt](const CABINDB_NAMESPACE::Slice& key,
                                            std::string* value) {
    return wbwi->GetFromBatch(cf_handle, *dbopt, key, value);
  };

  return CABINDB_NAMESPACE::JniUtil::v_op(getter, env, jkey, jkey_len);
}

/*
 * Class:     org_cabindb_WriteBatchWithIndex
 * Method:    getFromBatchAndDB
 * Signature: (JJJ[BI)[B
 */
jbyteArray Java_org_cabindb_WriteBatchWithIndex_getFromBatchAndDB__JJJ_3BI(
    JNIEnv* env, jobject /*jobj*/, jlong jwbwi_handle, jlong jdb_handle,
    jlong jreadopt_handle, jbyteArray jkey, jint jkey_len) {
  auto* wbwi =
      reinterpret_cast<CABINDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto* readopt =
      reinterpret_cast<CABINDB_NAMESPACE::ReadOptions*>(jreadopt_handle);

  auto getter = [&wbwi, &db, &readopt](const CABINDB_NAMESPACE::Slice& key,
                                       std::string* value) {
    return wbwi->GetFromBatchAndDB(db, *readopt, key, value);
  };

  return CABINDB_NAMESPACE::JniUtil::v_op(getter, env, jkey, jkey_len);
}

/*
 * Class:     org_cabindb_WriteBatchWithIndex
 * Method:    getFromBatchAndDB
 * Signature: (JJJ[BIJ)[B
 */
jbyteArray Java_org_cabindb_WriteBatchWithIndex_getFromBatchAndDB__JJJ_3BIJ(
    JNIEnv* env, jobject /*jobj*/, jlong jwbwi_handle, jlong jdb_handle,
    jlong jreadopt_handle, jbyteArray jkey, jint jkey_len, jlong jcf_handle) {
  auto* wbwi =
      reinterpret_cast<CABINDB_NAMESPACE::WriteBatchWithIndex*>(jwbwi_handle);
  auto* db = reinterpret_cast<CABINDB_NAMESPACE::DB*>(jdb_handle);
  auto* readopt =
      reinterpret_cast<CABINDB_NAMESPACE::ReadOptions*>(jreadopt_handle);
  auto* cf_handle =
      reinterpret_cast<CABINDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);

  auto getter = [&wbwi, &db, &cf_handle, &readopt](
                    const CABINDB_NAMESPACE::Slice& key, std::string* value) {
    return wbwi->GetFromBatchAndDB(db, *readopt, cf_handle, key, value);
  };

  return CABINDB_NAMESPACE::JniUtil::v_op(getter, env, jkey, jkey_len);
}

/*
 * Class:     org_cabindb_WriteBatchWithIndex
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_cabindb_WriteBatchWithIndex_disposeInternal(JNIEnv* /*env*/,
                                                          jobject /*jobj*/,
                                                          jlong handle) {
  auto* wbwi =
      reinterpret_cast<CABINDB_NAMESPACE::WriteBatchWithIndex*>(handle);
  assert(wbwi != nullptr);
  delete wbwi;
}

/* WBWICabinIterator below */

/*
 * Class:     org_cabindb_WBWICabinIterator
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_cabindb_WBWICabinIterator_disposeInternal(JNIEnv* /*env*/,
                                                        jobject /*jobj*/,
                                                        jlong handle) {
  auto* it = reinterpret_cast<CABINDB_NAMESPACE::WBWIIterator*>(handle);
  assert(it != nullptr);
  delete it;
}

/*
 * Class:     org_cabindb_WBWICabinIterator
 * Method:    isValid0
 * Signature: (J)Z
 */
jboolean Java_org_cabindb_WBWICabinIterator_isValid0(JNIEnv* /*env*/,
                                                     jobject /*jobj*/,
                                                     jlong handle) {
  return reinterpret_cast<CABINDB_NAMESPACE::WBWIIterator*>(handle)->Valid();
}

/*
 * Class:     org_cabindb_WBWICabinIterator
 * Method:    seekToFirst0
 * Signature: (J)V
 */
void Java_org_cabindb_WBWICabinIterator_seekToFirst0(JNIEnv* /*env*/,
                                                     jobject /*jobj*/,
                                                     jlong handle) {
  reinterpret_cast<CABINDB_NAMESPACE::WBWIIterator*>(handle)->SeekToFirst();
}

/*
 * Class:     org_cabindb_WBWICabinIterator
 * Method:    seekToLast0
 * Signature: (J)V
 */
void Java_org_cabindb_WBWICabinIterator_seekToLast0(JNIEnv* /*env*/,
                                                    jobject /*jobj*/,
                                                    jlong handle) {
  reinterpret_cast<CABINDB_NAMESPACE::WBWIIterator*>(handle)->SeekToLast();
}

/*
 * Class:     org_cabindb_WBWICabinIterator
 * Method:    next0
 * Signature: (J)V
 */
void Java_org_cabindb_WBWICabinIterator_next0(JNIEnv* /*env*/, jobject /*jobj*/,
                                              jlong handle) {
  reinterpret_cast<CABINDB_NAMESPACE::WBWIIterator*>(handle)->Next();
}

/*
 * Class:     org_cabindb_WBWICabinIterator
 * Method:    prev0
 * Signature: (J)V
 */
void Java_org_cabindb_WBWICabinIterator_prev0(JNIEnv* /*env*/, jobject /*jobj*/,
                                              jlong handle) {
  reinterpret_cast<CABINDB_NAMESPACE::WBWIIterator*>(handle)->Prev();
}

/*
 * Class:     org_cabindb_WBWICabinIterator
 * Method:    seek0
 * Signature: (J[BI)V
 */
void Java_org_cabindb_WBWICabinIterator_seek0(JNIEnv* env, jobject /*jobj*/,
                                              jlong handle, jbyteArray jtarget,
                                              jint jtarget_len) {
  auto* it = reinterpret_cast<CABINDB_NAMESPACE::WBWIIterator*>(handle);
  jbyte* target = env->GetByteArrayElements(jtarget, nullptr);
  if (target == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  CABINDB_NAMESPACE::Slice target_slice(reinterpret_cast<char*>(target),
                                        jtarget_len);

  it->Seek(target_slice);

  env->ReleaseByteArrayElements(jtarget, target, JNI_ABORT);
}

/*
 * Class:     org_cabindb_WBWICabinIterator
 * Method:    seekDirect0
 * Signature: (JLjava/nio/ByteBuffer;II)V
 */
void Java_org_cabindb_WBWICabinIterator_seekDirect0(
    JNIEnv* env, jobject /*jobj*/, jlong handle, jobject jtarget,
    jint jtarget_off, jint jtarget_len) {
  auto* it = reinterpret_cast<CABINDB_NAMESPACE::WBWIIterator*>(handle);
  auto seek = [&it](CABINDB_NAMESPACE::Slice& target_slice) {
    it->Seek(target_slice);
  };
  CABINDB_NAMESPACE::JniUtil::k_op_direct(seek, env, jtarget, jtarget_off,
                                          jtarget_len);
}

/*
 * Class:     org_cabindb_WBWICabinIterator
 * Method:    seekForPrev0
 * Signature: (J[BI)V
 */
void Java_org_cabindb_WBWICabinIterator_seekForPrev0(JNIEnv* env,
                                                     jobject /*jobj*/,
                                                     jlong handle,
                                                     jbyteArray jtarget,
                                                     jint jtarget_len) {
  auto* it = reinterpret_cast<CABINDB_NAMESPACE::WBWIIterator*>(handle);
  jbyte* target = env->GetByteArrayElements(jtarget, nullptr);
  if (target == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }

  CABINDB_NAMESPACE::Slice target_slice(reinterpret_cast<char*>(target),
                                        jtarget_len);

  it->SeekForPrev(target_slice);

  env->ReleaseByteArrayElements(jtarget, target, JNI_ABORT);
}

/*
 * Class:     org_cabindb_WBWICabinIterator
 * Method:    status0
 * Signature: (J)V
 */
void Java_org_cabindb_WBWICabinIterator_status0(JNIEnv* env, jobject /*jobj*/,
                                                jlong handle) {
  auto* it = reinterpret_cast<CABINDB_NAMESPACE::WBWIIterator*>(handle);
  CABINDB_NAMESPACE::Status s = it->status();

  if (s.ok()) {
    return;
  }

  CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_cabindb_WBWICabinIterator
 * Method:    entry1
 * Signature: (J)[J
 */
jlongArray Java_org_cabindb_WBWICabinIterator_entry1(JNIEnv* env,
                                                     jobject /*jobj*/,
                                                     jlong handle) {
  auto* it = reinterpret_cast<CABINDB_NAMESPACE::WBWIIterator*>(handle);
  const CABINDB_NAMESPACE::WriteEntry& we = it->Entry();

  jlong results[3];

  // set the type of the write entry
  results[0] = CABINDB_NAMESPACE::WriteTypeJni::toJavaWriteType(we.type);

  // NOTE: key_slice and value_slice will be freed by
  // org.cabindb.DirectSlice#close

  auto* key_slice = new CABINDB_NAMESPACE::Slice(we.key.data(), we.key.size());
  results[1] = reinterpret_cast<jlong>(key_slice);
  if (we.type == CABINDB_NAMESPACE::kDeleteRecord ||
      we.type == CABINDB_NAMESPACE::kSingleDeleteRecord ||
      we.type == CABINDB_NAMESPACE::kLogDataRecord) {
    // set native handle of value slice to null if no value available
    results[2] = 0;
  } else {
    auto* value_slice =
        new CABINDB_NAMESPACE::Slice(we.value.data(), we.value.size());
    results[2] = reinterpret_cast<jlong>(value_slice);
  }

  jlongArray jresults = env->NewLongArray(3);
  if (jresults == nullptr) {
    // exception thrown: OutOfMemoryError
    if (results[2] != 0) {
      auto* value_slice =
          reinterpret_cast<CABINDB_NAMESPACE::Slice*>(results[2]);
      delete value_slice;
    }
    delete key_slice;
    return nullptr;
  }

  env->SetLongArrayRegion(jresults, 0, 3, results);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    env->DeleteLocalRef(jresults);
    if (results[2] != 0) {
      auto* value_slice =
          reinterpret_cast<CABINDB_NAMESPACE::Slice*>(results[2]);
      delete value_slice;
    }
    delete key_slice;
    return nullptr;
  }

  return jresults;
}

/*
 * Class:     org_cabindb_WBWICabinIterator
 * Method:    refresh0
 * Signature: (J)V
 */
void Java_org_cabindb_WBWICabinIterator_refresh0(JNIEnv* env) {
  CABINDB_NAMESPACE::Status s = CABINDB_NAMESPACE::Status::NotSupported("Refresh() is not supported");
  CABINDB_NAMESPACE::CabinDBExceptionJni::ThrowNew(env, s);
}
