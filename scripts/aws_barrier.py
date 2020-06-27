#!/usr/local/bin/python3

'''
This script is used to generate commands to plot Figure 5 - the barrier study
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
  zipf = 0.0
  keys = 40000
  cross_ratio = 100
  barrier_delayed_percents = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000] # out of 1K
  barrier_artificial_delay_mss = [1000000, 500000, 200000, 100000, 50000, 20000, 10000] # ms

  # kiva
  for barrier_delayed_percent in barrier_delayed_percents:
    for barrier_artificial_delay_ms in barrier_artificial_delay_mss:
      assert barrier_artificial_delay_ms % barrier_delayed_percent == 0
      tts = barrier_artificial_delay_ms // barrier_delayed_percent
      for i in range(REPEAT):
        cmd = get_cmd_string(machine_id, ips, port + i)
        print(f'./bench_ycsb --logtostderr=1 --id={machine_id} --servers="{cmd}" --protocol=Aria --partition_num={partition_num} --threads={threads} --batch_size={batch_size} --read_write_ratio={read_write_ratio} --cross_ratio={cross_ratio} --keys={keys} --zipf={zipf} --barrier_delayed_percent={barrier_delayed_percent} --barrier_artificial_delay_ms={tts}')


def main():
  # ycsb
  print_ycsb()

if __name__ == '__main__':
  main()
