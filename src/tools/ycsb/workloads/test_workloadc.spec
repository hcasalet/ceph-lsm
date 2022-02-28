# Yahoo! Cloud System Benchmark
# Workload C: Read only
#   Application example: user profile cache, where profiles are constructed elsewhere (e.g., Hadoop)
#                        
#   Read/update ratio: 100/0
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian
keylength=16
fieldcount=8
fieldlength=16
poolname=cephlsm
table=cephlsm/level-0/keyrange-0/columngroup-0

recordcount=100000
operationcount=10000
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=false

readproportion=1
updateproportion=0
scanproportion=0
insertproportion=0




