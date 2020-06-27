#!/usr/local/bin/python3

'''
This script is used to generate commands to plot Figure 9 -- Performance on YCSB-A and YCSB-B, and Figure 11 --  Effectiveness of deterministic reordering.
Systems: Kiva, Bohm and Calvin
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
partition_num = threads
batch_size = 1000

def print_ycsb():
    
  read_write_ratio = 80
  zipfs = ["0.0", "0.1", "0.2", "0.3", "0.4", "0.5", "0.6", "0.7", "0.8", "0.9", "0.999"]
  keys = 40000
  cross_ratio = 100


  # global key space
  
  # aria
  for zipf in zipfs:
    for i in range(REPEAT):
      cmd = get_cmd_string(machine_id, ips, port + i)
      print(f'./bench_ycsb --logtostderr=1 --id={machine_id} --servers="{cmd}" --protocol=Aria --partition_num={partition_num} --threads={threads} --batch_size={batch_size} --read_write_ratio={read_write_ratio} --cross_ratio={cross_ratio} --keys={keys} --zipf={zipf} --global_key_space=True')

  # aria w/o reordering
  for zipf in zipfs:
    for i in range(REPEAT):
      cmd = get_cmd_string(machine_id, ips, port + i)
      print(f'./bench_ycsb --logtostderr=1 --id={machine_id} --servers="{cmd}" --protocol=Aria --partition_num={partition_num} --threads={threads} --batch_size={batch_size} --read_write_ratio={read_write_ratio} --cross_ratio={cross_ratio} --keys={keys} --zipf={zipf} --aria_reordering=false --global_key_space=True')

  # bohm
  for zipf in zipfs:
    for i in range(REPEAT):
      cmd = get_cmd_string(machine_id, ips, port + i)
      print(f'./bench_ycsb --logtostderr=1 --id={machine_id} --servers="{cmd}" --protocol=Bohm --partition_num={partition_num} --threads={threads} --batch_size={batch_size} --read_write_ratio={read_write_ratio} --cross_ratio={cross_ratio} --keys={keys} --zipf={zipf} --mvcc=True --bohm_single_spin=True --same_batch=False --global_key_space=True')
      
  # pwv
  for zipf in zipfs:
    for i in range(REPEAT):
      cmd = get_cmd_string(machine_id, ips, port + i)
      print(f'./bench_ycsb --logtostderr=1 --id={machine_id} --servers="{cmd}" --protocol=Pwv --partition_num={partition_num} --threads={threads} --batch_size={batch_size} --read_write_ratio={read_write_ratio} --cross_ratio={cross_ratio} --keys={keys} --zipf={zipf} --same_batch=False --global_key_space=True')

  # pwv dependent
  for zipf in zipfs:
    for i in range(REPEAT):
      cmd = get_cmd_string(machine_id, ips, port + i)
      print(f'./bench_ycsb --logtostderr=1 --id={machine_id} --servers="{cmd}" --protocol=Pwv --partition_num={partition_num} --threads={threads} --batch_size={batch_size} --read_write_ratio={read_write_ratio} --cross_ratio={cross_ratio} --keys={keys} --zipf={zipf} --pwv_ycsb_star=True --same_batch=False --global_key_space=True')


  # calvin
  n_lock_managers = [1, 2, 3, 4, 6]
  for n_lock_manager in n_lock_managers:
    for zipf in zipfs:
      for i in range(REPEAT):
        cmd = get_cmd_string(machine_id, ips, port + i)
        print(f'./bench_ycsb --logtostderr=1 --id={machine_id} --servers="{cmd}" --protocol=Calvin --partition_num={partition_num} --threads={threads} --batch_size={batch_size} --read_write_ratio={read_write_ratio} --cross_ratio={cross_ratio} --keys={keys} --zipf={zipf} --lock_manager={n_lock_manager} --replica_group=1 --same_batch=False --global_key_space=True')


  # ariaFB
  n_lock_managers = [1, 2, 3, 4, 6]
  for n_lock_manager in n_lock_managers:
    for zipf in zipfs:
      for i in range(REPEAT):
        cmd = get_cmd_string(machine_id, ips, port + i)
        print(f'./bench_ycsb --logtostderr=1 --id={machine_id} --servers="{cmd}" --protocol=AriaFB --partition_num={partition_num} --threads={threads} --batch_size={batch_size} --read_write_ratio={read_write_ratio} --cross_ratio={cross_ratio} --keys={keys} --zipf={zipf} --ariaFB_lock_manager={n_lock_manager} --global_key_space=True')


def main():
  # ycsb
  print_ycsb()

if __name__ == '__main__':
  main()
