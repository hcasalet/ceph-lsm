//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "port/stack_trace.h"

#if defined(CABINDB_LITE) ||                                                  \
    !(defined(CABINDB_BACKTRACE) || defined(OS_MACOSX)) || defined(CYGWIN) || \
    defined(OS_FREEBSD) || defined(OS_SOLARIS) || defined(OS_WIN)

// noop

namespace CABINDB_NAMESPACE {
namespace port {
void InstallStackTraceHandler() {}
void PrintStack(int /*first_frames_to_skip*/) {}
void PrintAndFreeStack(void* /*callstack*/, int /*num_frames*/) {}
void* SaveStack(int* /*num_frames*/, int /*first_frames_to_skip*/) {
  return nullptr;
}
}  // namespace port
}  // namespace CABINDB_NAMESPACE

#else

#include <execinfo.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <cxxabi.h>

namespace CABINDB_NAMESPACE {
namespace port {

namespace {

#if defined(OS_LINUX) || defined(OS_FREEBSD) || defined(OS_GNU_KFREEBSD)
const char* GetExecutableName() {
  static char name[1024];

  char link[1024];
  snprintf(link, sizeof(link), "/proc/%d/exe", getpid());
  auto read = readlink(link, name, sizeof(name) - 1);
  if (-1 == read) {
    return nullptr;
  } else {
    name[read] = 0;
    return name;
  }
}

void PrintStackTraceLine(const char* symbol, void* frame) {
  static const char* executable = GetExecutableName();
  if (symbol) {
    fprintf(stderr, "%s ", symbol);
  }
  if (executable) {
    // out source to addr2line, for the address translation
    const int kLineMax = 256;
    char cmd[kLineMax];
    snprintf(cmd, kLineMax, "addr2line %p -e %s -f -C 2>&1", frame, executable);
    auto f = popen(cmd, "r");
    if (f) {
      char line[kLineMax];
      while (fgets(line, sizeof(line), f)) {
        line[strlen(line) - 1] = 0;  // remove newline
        fprintf(stderr, "%s\t", line);
      }
      pclose(f);
    }
  } else {
    fprintf(stderr, " %p", frame);
  }

  fprintf(stderr, "\n");
}
#elif defined(OS_MACOSX)

void PrintStackTraceLine(const char* symbol, void* frame) {
  static int pid = getpid();
  // out source to atos, for the address translation
  const int kLineMax = 256;
  char cmd[kLineMax];
  snprintf(cmd, kLineMax, "xcrun atos %p -p %d  2>&1", frame, pid);
  auto f = popen(cmd, "r");
  if (f) {
    char line[kLineMax];
    while (fgets(line, sizeof(line), f)) {
      line[strlen(line) - 1] = 0;  // remove newline
      fprintf(stderr, "%s\t", line);
    }
    pclose(f);
  } else if (symbol) {
    fprintf(stderr, "%s ", symbol);
  }

  fprintf(stderr, "\n");
}

#endif

}  // namespace

void PrintStack(void* frames[], int num_frames) {
  auto symbols = backtrace_symbols(frames, num_frames);

  for (int i = 0; i < num_frames; ++i) {
    fprintf(stderr, "#%-2d  ", i);
    PrintStackTraceLine((symbols != nullptr) ? symbols[i] : nullptr, frames[i]);
  }
  free(symbols);
}

void PrintStack(int first_frames_to_skip) {
  const int kMaxFrames = 100;
  void* frames[kMaxFrames];

  auto num_frames = backtrace(frames, kMaxFrames);
  PrintStack(&frames[first_frames_to_skip], num_frames - first_frames_to_skip);
}

void PrintAndFreeStack(void* callstack, int num_frames) {
  PrintStack(static_cast<void**>(callstack), num_frames);
  free(callstack);
}

void* SaveStack(int* num_frames, int first_frames_to_skip) {
  const int kMaxFrames = 100;
  void* frames[kMaxFrames];

  auto count = backtrace(frames, kMaxFrames);
  *num_frames = count - first_frames_to_skip;
  void* callstack = malloc(sizeof(void*) * *num_frames);
  memcpy(callstack, &frames[first_frames_to_skip], sizeof(void*) * *num_frames);
  return callstack;
}

static void StackTraceHandler(int sig) {
  // reset to default handler
  signal(sig, SIG_DFL);
  fprintf(stderr, "Received signal %d (%s)\n", sig, strsignal(sig));
  // skip the top three signal handler related frames
  PrintStack(3);
  // re-signal to default handler (so we still get core dump if needed...)
  raise(sig);
}

void InstallStackTraceHandler() {
  // just use the plain old signal as it's simple and sufficient
  // for this use case
  signal(SIGILL, StackTraceHandler);
  signal(SIGSEGV, StackTraceHandler);
  signal(SIGBUS, StackTraceHandler);
  signal(SIGABRT, StackTraceHandler);
}

}  // namespace port
}  // namespace CABINDB_NAMESPACE

#endif
