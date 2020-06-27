#!/bin/bash

rm -rf CMakeFiles/ CMakeCache.txt goo*
cmake -DCMAKE_BUILD_TYPE=Release .
make -j
