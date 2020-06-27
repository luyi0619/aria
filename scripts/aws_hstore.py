#!/usr/local/bin/python3

'''
This script is used to generate commands to plot Figure 10(a) - hstore

Systems: HStore
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

assert n_machines == 1, "we are expecting there are 2 machines."

threads = 12
partition_num = threads

def print_ycsb():
    
  read_write_ratio = 80
  zipf = 0.0
  keys = 40000
  cross_ratio = 0

  # s2pl
  for i in range(REPEAT):
    cmd = get_cmd_string(machine_id, ips, port + i)
    print(f'./bench_ycsb --logtostderr=1 --id={machine_id} --servers="{cmd}" --protocol=TwoPL --partition_num={partition_num} --threads={threads}  --read_write_ratio={read_write_ratio} --cross_ratio={cross_ratio} --keys={keys} --zipf={zipf}')

def main():
  # ycsb
  print_ycsb()

if __name__ == '__main__':
  main()
