#!/usr/local/bin/python3

import sys
import os

from utility import load_ips

assert len(sys.argv) == 2, "this script expects one parameter"
zip_file = sys.argv[1] 

ips = load_ips('ips.txt')
n_machines = len(ips) # 8
# assert n_machines == 8, "we are expecting there are 8 machines."

for ip in ips:
    external_ip = ip[1]
    os.system("scp %s ubuntu@%s:~/" %(zip_file, external_ip))
