#!/bin/bash

pushd /root/spark-perf

function run_test {
  mcs=$1
  scale_base=$2
  scale=`echo "$scale_base" | bc -l`
  echo $scale
  cat config/config.py | sed "s/^SCALE_FACTOR.*/SCALE_FACTOR=$scale/g" > config/this.config 
  cp config/this.config config/config.py
  
  num_workers=$mcs
  cp /root/spark-ec2/slaves /root/spark/conf/slaves
  /root/spark/sbin/stop-all.sh
  sleep 2
  head -n $num_workers /root/spark-ec2/slaves > /root/spark/conf/slaves
  /root/spark/sbin/start-all.sh
  sleep 5
  
  ./bin/run
}

#run_test 32 1.0
#run_test 45 1.0
#run_test 64 1.0

# Expt design
# 7.0000    0.0078
# 5.0000    0.0060
#16.0000    0.0171
# 6.0000    0.0060
# 1.0000    0.0023

#run_test 1 0.0023
#run_test 6 0.0060
#run_test 16 0.0171
#run_test 5 0.0060
#run_test 7 0.0078

# Cost
#12.0000    0.0115
#10.0000    0.0097
# 8.0000    0.0078
# 6.0000    0.0060
#15.0000    0.0152
#13.0000    0.0134

#run_test 12 0.0115
#run_test 10 0.0097
#run_test 8 0.0078
##run_test 6 0.0060
#run_test 15 0.0152
#run_test 13 0.0134

#run_test 16 0.1
#run_test 16 0.0208
#run_test 16 0.05
#run_test 16 0.0189
#run_test 1 0.0041
#run_test 6 0.0078
#run_test 8 0.0078
#run_test 4 0.0041

#run_test 16 0.0787
#run_test  6 0.0302
#run_test  5 0.0244
#run_test  6 0.0283
#run_test  1 0.0069
#run_test  1 0.0050
run_test 16 0.5
