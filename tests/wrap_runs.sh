#!/bin/bash
#
# Copyright (C) 2019, Northwestern University and Argonne National Laboratory
# See COPYRIGHT notice in top-level directory.
#

NSERVER=4
NCLIENT=1

OUT_FILE=`basename $1`

# Would be nice to be ale to tie into SLURM or other PMIx-aware facilities
prte --host localhost:8 &

# Start server
sleep 1
echo "prun -np ${NSERVER} src/provider/bv-server -p sockets -b 2048 -f ${OUT_FILE}.svc &"
prun -np ${NSERVER} src/provider/bv-server -p sockets -b 2048 -f ${OUT_FILE}.svc &

echo "SERVER_PID=$!"
SERVER_PID=$!

# Wait for server to create group file
until [ -f ${OUT_FILE}.svc ]
do
    echo "${OUT_FILE}.svc not found, wait for 1 sec."
    sleep 1
done

# Make sure server has finish editing group file
while :
do
    if ! [[ `lsof | grep ${OUT_FILE}.svc` ]]
    then
        break
    fi
    echo "Server is editing ${OUT_FILE}.svc, wait for 1 sec."
    sleep 1
done

# Run test program
echo "prun -np ${NCLIENT} $1 ${OUT_FILE}.svc ${OUT_FILE}.bin"
prun -np ${NCLIENT} $1 ${OUT_FILE}.svc ${OUT_FILE}.bin
RET_VAL=$?
echo "RET_VAL=${RET_VAL}"

# Stop server
echo "src/client/bv-shutdown ${OUT_FILE}.svc"
prun -np 1 src/client/bv-shutdown ${OUT_FILE}.svc
prun -terminate

# Return result
exit ${RET_VAL}
