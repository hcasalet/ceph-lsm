# shellcheck disable=SC2148
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
PLATFORM=64
if [ `getconf LONG_BIT` != "64" ]
then
  PLATFORM=32
fi

CABIN_JAR=`find target -name cabindbjni*.jar`

echo "Running benchmark in $PLATFORM-Bit mode."
# shellcheck disable=SC2068
java -server -d$PLATFORM -XX:NewSize=4m -XX:+AggressiveOpts -Djava.library.path=target -cp "${CABIN_JAR}:benchmark/target/classes" org.cabindb.benchmark.DbBenchmark $@
