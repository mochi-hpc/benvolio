#!/bin/sh
#
# Copyright (C) 2019, Northwestern University and Argonne National Laboratory
# See COPYRIGHT notice in top-level directory.
#

NSERVER=4
NCLIENT=1

OUT_FILE=`basename $1`

# Start server
echo "mpiexec -np ${NSERVER} src/provider/bv-server -p sockets -b 2048 -f ${OUT_FILE}.svc &"
mpiexec -np {NSERVER} src/provider/bv-server -p sockets -b 2048 -f ${OUT_FILE}.svc &

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
echo "mpiexec -np ${NCLIENT} $1 ${OUT_FILE}.svc ${OUT_FILE}.bin"
mpiexec -np ${NCLIENT} $1 ${OUT_FILE}.svc ${OUT_FILE}.bin
RET_VAL=$?
echo "RET_VAL=${RET_VAL}"

# Stop server
echo "src/client/bv-shutdown ${OUT_FILE}.svc"
src/client/bv-shutdown ${OUT_FILE}.svc

# Return result
exit ${RET_VAL}
