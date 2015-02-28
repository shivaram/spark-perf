#!/bin/bash

pushd /root/spark-perf

# Goal is to estimate time for scale = 2.0 with max of 64 machines

# Process
# First do some weak scaling profiling
#  Run 2/64*x for x = 1, 2, 4, 8, 16 machines
# Also do some strong scaling as well
#  For x = 4, 8, 16 machines run with scale = 0.5 
#
# Collect ground truth
#  For scale = 2.0 run x = 16, 20, 32, 45, 64

for mcs in 1 2 4 8 16
do
  scale=`echo "2.0/64*$mcs" | bc -l`
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
done

for mcs in 4 8 16
do
  scale=0.5
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
done

for mcs in 16 20 32 45 64
do
  scale=2.0
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
done
