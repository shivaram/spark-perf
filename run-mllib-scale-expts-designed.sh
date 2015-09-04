#!/bin/bash

pushd /root/spark-perf

function run_test {
  mcs=$1
  scale_base=$2
  scale=`echo "$scale_base*5.0" | bc -l`
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

run_test 32 1.0
run_test 45 1.0
run_test 64 1.0

# Expt design
#16.0000    0.0212
# 5.0000    0.0071
# 1.0000    0.0030
# 7.0000    0.0091
#16.0000    0.0192
#16.0000    0.0172
# 6.0000    0.0071
# 7.0000    0.0071
# 5.0000    0.0050
# 1.0000    0.0010

run_test 1 0.0010
run_test 5 0.0050
run_test 7 0.0071
run_test 6 0.0071
run_test 16 0.0172
run_test 16 0.0192
run_test 7 0.0091
run_test 1 0.0030
#run_test 5 0.0071
#run_test 16 0.0212

# Cost
# 1.0000    0.0010
# 3.0000    0.0030
# 5.0000    0.0050
# 7.0000    0.0071
# 9.0000    0.0091
#11.0000    0.0111
#13.0000    0.0131
#15.0000    0.0151
#16.0000    0.0172
#14.0000    0.0151

run_test 3 0.0030
run_test 9 0.0091
run_test 11 0.0111
run_test 13 0.0131
run_test 15 0.0151
run_test 14 0.0151

#run_test 45 1.6
#run_test 64 1.6

#run_test 16 0.02
#run_test 32 0.02
#run_test 64 0.02
#run_test 20 0.02
#run_test 40 0.02
#run_test 48 0.02
#run_test 56 0.02
