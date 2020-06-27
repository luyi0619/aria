#!/usr/local/bin/python3

'''
This script is used to generate commands to plot Figure 13 -- Performance of each system on TPC-C
Systems: Aria, AriaFB, Bohm, Calvin, and PWV
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

assert n_machines == 1, "we are expecting there is 1 machine."


threads = 12
batch_size = 500

def print_tpcc():
    
  query = 'mixed'
  neworder_dist = 10
  payment_dist = 15
  partition_nums = [1, 2, 4, 6, 8, 10, 12, 36, 60, 84, 108, 132, 156, 180]

  # aria
  for partition_num in partition_nums:
    for i in range(REPEAT):
      cmd = get_cmd_string(machine_id, ips, port + i)
      print(f'./bench_tpcc --logtostderr=1 --id={machine_id} --servers="{cmd}" --protocol=Aria --partition_num={partition_num} --threads={threads} --batch_size={batch_size} --query={query} --neworder_dist={neworder_dist} --payment_dist={payment_dist}')

  # bohm
  for partition_num in partition_nums:
    for i in range(REPEAT):
      cmd = get_cmd_string(machine_id, ips, port + i)
      print(f'./bench_tpcc --logtostderr=1 --id={machine_id} --servers="{cmd}" --protocol=Bohm --partition_num={partition_num} --threads={threads} --batch_size={batch_size} --query={query} --neworder_dist={neworder_dist} --payment_dist={payment_dist} --mvcc=True --bohm_single_spin=True --same_batch=False')

  # pwv
  for partition_num in partition_nums:
    for i in range(REPEAT):
      cmd = get_cmd_string(machine_id, ips, port + i)
      print(f'./bench_tpcc --logtostderr=1 --id={machine_id} --servers="{cmd}" --protocol=Pwv --partition_num={partition_num} --threads={threads} --batch_size={batch_size} --query={query} --neworder_dist={neworder_dist} --payment_dist={payment_dist} --same_batch=False')

  # calvin
  n_lock_managers = [1, 2, 3, 4, 6]
  for n_lock_manager in n_lock_managers:
    for partition_num in partition_nums:
      for i in range(REPEAT):
        cmd = get_cmd_string(machine_id, ips, port + i)
        print(f'./bench_tpcc --logtostderr=1 --id={machine_id} --servers="{cmd}" --protocol=Calvin --partition_num={partition_num} --threads={threads} --batch_size={batch_size} --query={query} --neworder_dist={neworder_dist} --payment_dist={payment_dist} --lock_manager={n_lock_manager} --replica_group=1 --same_batch=False')

  # ariaFB
  n_lock_managers = [1, 2, 3, 4, 6]
  for n_lock_manager in n_lock_managers:
    for partition_num in partition_nums:
      for i in range(REPEAT):
        cmd = get_cmd_string(machine_id, ips, port + i)
        print(f'./bench_tpcc --logtostderr=1 --id={machine_id} --servers="{cmd}" --protocol=AriaFB --partition_num={partition_num} --threads={threads} --batch_size={batch_size} --query={query} --neworder_dist={neworder_dist} --payment_dist={payment_dist} --same_batch=False --ariaFB_lock_manager={n_lock_manager}')

def main():
  # tpc-c
  print_tpcc()

if __name__ == '__main__':
  main()
