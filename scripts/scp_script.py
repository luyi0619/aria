#!/usr/local/bin/python3

import sys
import os

from utility import load_ips

assert len(sys.argv) == 3, "this script expects two parameters"
port = int(sys.argv[1]) 
script = sys.argv[2]

ips = load_ips('ips.txt')
n_machines = len(ips) # 8
#assert n_machines == 8, "we are expecting there are 8 machines."

for i in range(n_machines):
    external_ip = ips[i][1]
    os.system("python3 %s %d %d > run.sh" % (script, i, port))
    os.system("chmod u+x run.sh")
    os.system("scp run.sh ubuntu@%s:~/scar/run.sh" % external_ip)
