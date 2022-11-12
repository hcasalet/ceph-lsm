//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// JNI Callbacks from C++ to sub-classes or org.cabindb.CabinCallbackObject

#ifndef JAVA_CABINJNI_JNICALLBACK_H_
#define JAVA_CABINJNI_JNICALLBACK_H_

#include <jni.h>

#include "cabindb/cabindb_namespace.h"

namespace CABINDB_NAMESPACE {
class JniCallback {
 public:
  JniCallback(JNIEnv* env, jobject jcallback_obj);
  virtual ~JniCallback();

  const jobject& GetJavaObject() const { return m_jcallback_obj; }

 protected:
  JavaVM* m_jvm;
  jobject m_jcallback_obj;
  JNIEnv* getJniEnv(jboolean* attached) const;
  void releaseJniEnv(jboolean& attached) const;
  };
  }  // namespace CABINDB_NAMESPACE

#endif  // JAVA_CABINJNI_JNICALLBACK_H_
