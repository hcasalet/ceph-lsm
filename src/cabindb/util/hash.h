//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Common hash functions with convenient interfaces. If hashing a
// statically-sized input in a performance-critical context, consider
// calling a specific hash implementation directly, such as
// XXH3p_64bits from xxhash.h.
//
// Since this is a very common header, implementation details are kept
// out-of-line. Out-of-lining also aids in tracking the time spent in
// hashing functions. Inlining is of limited benefit for runtime-sized
// hash inputs.

#pragma once

#include <cstddef>
#include <cstdint>

#include "include/cabindb/slice.h"
#include "util/fastrange.h"

namespace CABINDB_NAMESPACE {

// Stable/persistent 64-bit hash. Higher quality and generally faster than
// Hash(), especially for inputs > 24 bytes.
// KNOWN FLAW: incrementing seed by 1 might not give sufficiently independent
// results from previous seed. Recommend incrementing by a large odd number.
extern uint64_t Hash64(const char* data, size_t n, uint64_t seed);

// Specific optimization without seed (same as seed = 0)
extern uint64_t Hash64(const char* data, size_t n);

// Non-persistent hash. Must only used for in-memory data structures.
// The hash results are thus subject to change between releases,
// architectures, build configuration, etc. (Thus, it rarely makes sense
// to specify a seed for this function, except for a "rolling" hash.)
// KNOWN FLAW: incrementing seed by 1 might not give sufficiently independent
// results from previous seed. Recommend incrementing by a large odd number.
inline uint64_t NPHash64(const char* data, size_t n, uint64_t seed) {
#ifdef CABINDB_MODIFY_NPHASH
  // For testing "subject to change"
  return Hash64(data, n, seed + 123456789);
#else
  // Currently same as Hash64
  return Hash64(data, n, seed);
#endif
}

// Specific optimization without seed (same as seed = 0)
inline uint64_t NPHash64(const char* data, size_t n) {
#ifdef CABINDB_MODIFY_NPHASH
  // For testing "subject to change"
  return Hash64(data, n, 123456789);
#else
  // Currently same as Hash64
  return Hash64(data, n);
#endif
}

// Stable/persistent 32-bit hash. Moderate quality and high speed on
// small inputs.
// TODO: consider rename to Hash32
// KNOWN FLAW: incrementing seed by 1 might not give sufficiently independent
// results from previous seed. Recommend pseudorandom or hashed seeds.
extern uint32_t Hash(const char* data, size_t n, uint32_t seed);

// TODO: consider rename to LegacyBloomHash32
inline uint32_t BloomHash(const Slice& key) {
  return Hash(key.data(), key.size(), 0xbc9f1d34);
}

inline uint64_t GetSliceHash64(const Slice& key) {
  return Hash64(key.data(), key.size());
}

inline uint64_t GetSliceNPHash64(const Slice& s) {
  return NPHash64(s.data(), s.size());
}

inline size_t GetSliceRangedNPHash(const Slice& s, size_t range) {
  return FastRange64(NPHash64(s.data(), s.size()), range);
}

// TODO: consider rename to GetSliceHash32
inline uint32_t GetSliceHash(const Slice& s) {
  return Hash(s.data(), s.size(), 397);
}

// Useful for splitting up a 64-bit hash
inline uint32_t Upper32of64(uint64_t v) {
  return static_cast<uint32_t>(v >> 32);
}
inline uint32_t Lower32of64(uint64_t v) { return static_cast<uint32_t>(v); }

// std::hash compatible interface.
// TODO: consider rename to SliceHasher32
struct SliceHasher {
  uint32_t operator()(const Slice& s) const { return GetSliceHash(s); }
};

}  // namespace CABINDB_NAMESPACE
