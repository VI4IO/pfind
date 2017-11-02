#!/bin/bash -e
# This script builds the optional parallel find

CC=mpicc
CFLAGS="-g -O2 -Wextra -Wall -pipe"

rm *.o 2>&1 || true

echo "Building parallel find"

$CC $CFLAGS -c src/pfind-main.c || exit 1
$CC $CFLAGS -c src/pfind-options.c || exit 1
#$CC $CFLAGS -I../libcircle/libcircle/ -c src/pfind.c || exit 1
$CC $CFLAGS -o pfind *.o ./libcircle/.libs/libcircle.a -lm  || exit 1

echo "[OK]"
