#!/usr/local/bin/python3

'''
This script is used to generate commands to plot Figure 12 -- Performance of each system on Scaled TPC-C
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

assert n_machines == 2, "we are expecting there is 2 machines."


threads = 12
partition_num = threads

def print_tpcc():
    
  query = 'mixed'
  neworder_dist = 10
  payment_dist = 15
  partition_nums = [1, 2, 4, 6, 8, 10, 12, 36, 60, 84, 108, 132, 156, 180]

  # s2pl
  for partition_num in partition_nums:
    for i in range(REPEAT):
      cmd = get_cmd_string(machine_id, ips, port + i)
      print(f'./bench_tpcc --logtostderr=1 --id={machine_id} --servers="{cmd}" --protocol=TwoPL --partitioner=pb --partition_num={partition_num} --threads={threads} --query={query} --neworder_dist={neworder_dist} --payment_dist={payment_dist}')
      


def main():
  # tpc-c
  print_tpcc()

if __name__ == '__main__':
  main()
