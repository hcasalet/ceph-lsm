# shellcheck disable=SC2148
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
TESTDIR=`mktemp -d ${TMPDIR:-/tmp}/cabindb-dump-test.XXXXX`
DUMPFILE="tools/sample-dump.dmp"

# Verify that the sample dump file is undumpable and then redumpable.
./cabindb_undump --dump_location=$DUMPFILE --db_path=$TESTDIR/db
./cabindb_dump --anonymous --db_path=$TESTDIR/db --dump_location=$TESTDIR/dump
cmp $DUMPFILE $TESTDIR/dump
