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
poolname=cephlsm
table=cephlsm

#recordcount=100000
#operationcount=10000

recordcount=8000
operationcount=8000
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=false

readproportion=0
updateproportion=1.0
scanproportion=0
insertproportion=0



