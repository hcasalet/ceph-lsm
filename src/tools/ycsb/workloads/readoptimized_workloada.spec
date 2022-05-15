# Yahoo! Cloud System Benchmark
# Workload A: Update heavy workload
#   Application example: Session store recording recent actions
#                        
#   Read/update ratio: 50/50
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian
keylength=16
fieldcount=8
fieldlength=16
poolname=readoptimized
table=readoptimized/level-0/keyrange-0/columngroup-0

recordcount=100
operationcount=10
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=false

readproportion=0.5
updateproportion=0.5
scanproportion=0
insertproportion=0
