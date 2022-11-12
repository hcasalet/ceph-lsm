//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#if !(defined GFLAGS) || defined(CABINDB_LITE)

#include <cstdio>
int main() {
#ifndef GFLAGS
  fprintf(stderr, "Please install gflags to run cabindb tools\n");
#endif
#ifdef CABINDB_LITE
  fprintf(stderr, "DbDumpTool is not supported in CABINDB_LITE\n");
#endif
  return 1;
}

#else

#include "include/cabindb/convenience.h"
#include "include/cabindb/db_dump_tool.h"
#include "util/gflags_compat.h"

DEFINE_string(db_path, "", "Path to the db that will be dumped");
DEFINE_string(dump_location, "", "Path to where the dump file location");
DEFINE_bool(anonymous, false,
            "Remove information like db path, creation time from dumped file");
DEFINE_string(db_options, "",
              "Options string used to open the database that will be dumped");

int main(int argc, char** argv) {
  GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_db_path == "" || FLAGS_dump_location == "") {
    fprintf(stderr, "Please set --db_path and --dump_location\n");
    return 1;
  }

  CABINDB_NAMESPACE::DumpOptions dump_options;
  dump_options.db_path = FLAGS_db_path;
  dump_options.dump_location = FLAGS_dump_location;
  dump_options.anonymous = FLAGS_anonymous;

  CABINDB_NAMESPACE::Options db_options;
  if (FLAGS_db_options != "") {
    CABINDB_NAMESPACE::Options parsed_options;
    CABINDB_NAMESPACE::Status s = CABINDB_NAMESPACE::GetOptionsFromString(
        db_options, FLAGS_db_options, &parsed_options);
    if (!s.ok()) {
      fprintf(stderr, "Cannot parse provided db_options\n");
      return 1;
    }
    db_options = parsed_options;
  }

  CABINDB_NAMESPACE::DbDumpTool tool;
  if (!tool.Run(dump_options, db_options)) {
    return 1;
  }
  return 0;
}
#endif  // !(defined GFLAGS) || defined(CABINDB_LITE)
