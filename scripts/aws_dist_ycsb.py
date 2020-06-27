#!/usr/local/bin/python3

'''
This script is used to generate commands to plot Figure 13(a) --  Performance of each system on YCSB and TPC-C in the distributed setting
Systems: Aria, AriaFB, Calvin, and S2PL
'''

import sys

from utility import load_ips
from utility import get_cmd_string

# repeat experiments for the following times
REPEAT = 5

assert len(sys.argv) == 3, "this script expects two parameters"

machine_id = int(sys.argv[1]) 
port = int(sys.argv[2]) 

ips = load_ips('ips.txt')

n_machines = len(ips)

assert n_machines == 8, "we are expecting there are 8 machines."

threads = 12
partition_num = threads * n_machines
batch_size = 10000

def print_ycsb():
    
  read_write_ratio = 80
  zipf = 0.0
  keys = 40000
  cross_ratios = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]

  # aria
  for cross_ratio in cross_ratios:
    for i in range(REPEAT):
      cmd = get_cmd_string(machine_id, ips, port + i)
      print(f'./bench_ycsb --logtostderr=1 --id={machine_id} --servers="{cmd}" --protocol=Aria --partition_num={partition_num} --threads={threads} --batch_size={batch_size} --read_write_ratio={read_write_ratio} --cross_ratio={cross_ratio} --keys={keys} --zipf={zipf} --two_partitions=True')

  # S2PL
  for cross_ratio in cross_ratios:
    for i in range(REPEAT):
      cmd = get_cmd_string(machine_id, ips, port + i)
      print(f'./bench_ycsb --logtostderr=1 --id={machine_id} --servers="{cmd}" --protocol=TwoPL --partitioner=hash2 --partition_num={partition_num} --threads={threads} --read_write_ratio={read_write_ratio} --cross_ratio={cross_ratio} --keys={keys} --zipf={zipf} --two_partitions=True')

  # calvin
  n_lock_managers = [1, 2, 3, 4, 6]
  for n_lock_manager in n_lock_managers:
    for cross_ratio in cross_ratios:
      for i in range(REPEAT):
        cmd = get_cmd_string(machine_id, ips, port + i)
        print(f'./bench_ycsb --logtostderr=1 --id={machine_id} --servers="{cmd}" --protocol=Calvin --partition_num={partition_num} --threads={threads} --batch_size={batch_size * n_machines} --read_write_ratio={read_write_ratio} --cross_ratio={cross_ratio} --keys={keys} --zipf={zipf} --lock_manager={n_lock_manager} --replica_group={n_machines} --same_batch=True --two_partitions=True')

  # ariaFB
  n_lock_managers = [1, 2, 3, 4, 6]
  for n_lock_manager in n_lock_managers:
    for cross_ratio in cross_ratios:
      for i in range(REPEAT):
        cmd = get_cmd_string(machine_id, ips, port + i)
        print(f'./bench_ycsb --logtostderr=1 --id={machine_id} --servers="{cmd}" --protocol=AriaFB --partition_num={partition_num} --threads={threads} --batch_size={batch_size * n_machines // 2} --read_write_ratio={read_write_ratio} --cross_ratio={cross_ratio} --keys={keys} --zipf={zipf} --ariaFB_lock_manager={n_lock_manager} --same_batch=True --two_partitions=True')

def main():
  # ycsb
  print_ycsb()

if __name__ == '__main__':
  main()
