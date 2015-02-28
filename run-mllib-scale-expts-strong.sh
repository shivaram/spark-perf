#!/bin/bash

pushd /root/spark-perf

# Scale 2.0 works fine with 63 slaves, so revise accordingly

for scale in 0.5
do
  cat config/config.py | sed "s/^SCALE_FACTOR.*/SCALE_FACTOR=$scale/g" > config/this.config 
  cp config/this.config config/config.py


  for mcs in 4 8 16 20 32 45 64
  do
    num_workers=$mcs
    cp /root/spark-ec2/slaves /root/spark/conf/slaves
    /root/spark/sbin/stop-all.sh
    sleep 2
    head -n $num_workers /root/spark-ec2/slaves > /root/spark/conf/slaves
    /root/spark/sbin/start-all.sh
    sleep 5

    ./bin/run
  done
done
