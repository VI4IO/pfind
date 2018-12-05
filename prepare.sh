#!/bin/bash

echo "Download LZ4 used for optimizing the network traffic"

git clone https://github.com/lz4/lz4.git
cd lz4
make -j
