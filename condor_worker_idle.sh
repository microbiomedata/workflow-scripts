#!/bin/bash

if [ "z$SLURM_PROCID" = "z0" ] ; then
	~/bin/post2slack "Started $SLURM_JOBID with $SLURM_NNODES nodes"
fi


mkdir -p $SCRATCH/condor/$(hostname)/log
mkdir -p $SCRATCH/condor/$(hostname)/execute
condor_master &
sleep 5
export MASTER_PID=$(ps aux|grep condor_master|grep -v grep|awk '{print $2}')

# Give things time to start
sleep 20
/global/cfs/cdirs/m3408/aim2/tools/cull.py
# Just in case
kill $MASTER_PID
wait
