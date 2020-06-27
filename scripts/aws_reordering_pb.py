#!/usr/local/bin/python3

'''
This script is used to generate commands to plot Figure 9 -- Performance on YCSB-A and YCSB-B, and Figure 11 --  Effectiveness of deterministic reordering.
Systems: PB
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

assert n_machines == 2, "we are expecting there are 2 machines."

threads = 12
partition_num = threads
batch_size = 10000

def print_ycsb():
    
  read_write_ratio = 80
  zipfs = ["0.0", "0.1", "0.2", "0.3", "0.4", "0.5", "0.6", "0.7", "0.8", "0.9", "0.999"]
  keys = 40000
  cross_ratio = 100

  # TwoPL
  for zipf in zipfs:
    for i in range(REPEAT):
      cmd = get_cmd_string(machine_id, ips, port + i)
      print(f'./bench_ycsb --logtostderr=1 --id={machine_id} --servers="{cmd}" --protocol=TwoPL --partition_num={partition_num} --threads={threads}  --partitioner=pb --read_write_ratio={read_write_ratio} --cross_ratio={cross_ratio} --keys={keys} --zipf={zipf} --global_key_space=True')


def main():
  # ycsb
  print_ycsb()

if __name__ == '__main__':
  main()
