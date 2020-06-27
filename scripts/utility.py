#!/usr/local/bin/python3

def load_ips(ip_file_path):
    ip_file = open(ip_file_path, 'r')
    ips = [line.strip().split('\t') for line in ip_file if line.startswith('#') == False]
    return ips

def get_cmd_string(id, ips, port):
    n_nodes = len(ips)
    assert id < n_nodes, 'id should be less than length of ips'
    cmd = ''
    for i in range(n_nodes):
        cmd += ';' if i > 0 else ''
        (internal_ip, external_ip) = ips[i]
        if id == i:
            cmd += internal_ip + ':' + str(port)
        else:
            cmd += external_ip + ':' + str(port)
    return cmd