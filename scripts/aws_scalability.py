#!/usr/local/bin/python3

'''
This script is used to generate commands to plot Figure 14 -- Scalability of Kiva on YCSB benchmark
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

assert n_machines == 16, "we are expecting there is 16 machines."

threads = 12
batch_size = 10000

def print_ycsb():
    
  read_write_ratio = 80
  zipf = 0.0
  keys = 40000
  cross_ratios = [0, 1, 5, 10, 20]
  n_nodes = [16, 14, 12, 10, 8, 6, 4, 2]

  for n_node in n_nodes:
    if machine_id >= n_node:
      break
    partition_num = threads * n_node
    for cross_ratio in cross_ratios:
      for i in range(REPEAT):
        cmd = get_cmd_string(machine_id, ips[:n_node], port + i)
        print(f'./bench_ycsb --logtostderr=1 --id={machine_id} --servers="{cmd}" --protocol=Aria --partition_num={partition_num} --threads={threads} --batch_size={batch_size} --read_write_ratio={read_write_ratio} --cross_ratio={cross_ratio} --keys={keys} --zipf={zipf} --two_partitions=True')

def main():
  # ycsb
  print_ycsb()

if __name__ == '__main__':
  main()
