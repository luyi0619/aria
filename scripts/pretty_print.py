#!/usr/local/bin/python3

import sys

assert len(sys.argv) == 4, "this script expects three parameters"

n_rows = int(sys.argv[1])
n_columns = int(sys.argv[2])
file_path = sys.argv[3]

def main():
    lines = [line.strip() for line in open(file_path, 'r')]
    # remove not finished lines
    n_blocks = len(lines) // (n_rows * n_columns)
    lines = lines[:n_blocks * n_rows * n_columns]


    data = [[] for i in range(n_columns)]
    
    # fill in data
    cursor = -1

    for i in range(len(lines)):
        if i % n_rows == 0:
            cursor += 1

        if cursor == n_columns:
            cursor = 0
    
        data[cursor].append(lines[i])
    
    # print
    
    for i in range(len(data[0])):
        for j in range(n_columns):
            sys.stdout.write(data[j][i])            
            sys.stdout.write( '\n' if j == n_columns - 1 else '\t')

if __name__ == '__main__':
    main()


