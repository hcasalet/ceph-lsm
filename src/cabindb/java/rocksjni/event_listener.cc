//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// cabindb::EventListener.

#include <jni.h>

#include <memory>

#include "include/org_cabindb_AbstractEventListener.h"
#include "cabinjni/event_listener_jnicallback.h"
#include "cabinjni/portal.h"

/*
 * Class:     org_cabindb_AbstractEventListener
 * Method:    createNewEventListener
 * Signature: (J)J
 */
jlong Java_org_cabindb_AbstractEventListener_createNewEventListener(
    JNIEnv* env, jobject jobj, jlong jenabled_event_callback_values) {
  auto enabled_event_callbacks =
      CABINDB_NAMESPACE::EnabledEventCallbackJni::toCppEnabledEventCallbacks(
          jenabled_event_callback_values);
  auto* sptr_event_listener =
      new std::shared_ptr<CABINDB_NAMESPACE::EventListener>(
          new CABINDB_NAMESPACE::EventListenerJniCallback(
              env, jobj, enabled_event_callbacks));
  return reinterpret_cast<jlong>(sptr_event_listener);
}

/*
 * Class:     org_cabindb_AbstractEventListener
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_cabindb_AbstractEventListener_disposeInternal(JNIEnv*, jobject,
                                                            jlong jhandle) {
  delete reinterpret_cast<std::shared_ptr<CABINDB_NAMESPACE::EventListener>*>(
      jhandle);
}
