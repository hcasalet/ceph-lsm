# Yahoo! Cloud System Benchmark
# Workload A: Update heavy workload
#   Application example: Session store recording recent actions
#                        
#   Read/update ratio: 50/50
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian
keylength=16
fieldcount=10
fieldlength=100
poolname=cephlsm
table=cephlsm1

recordcount=100000
operationcount=100000

workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=false

readproportion=0.5
updateproportion=0.5
scanproportion=0
insertproportion=0
