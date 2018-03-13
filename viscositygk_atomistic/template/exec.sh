#!/bin/sh
#PBS -l walltime=5:00:00
#PBS -l select=1:ncpus=24:mem=1gb

module load intel-suite
module load lammps/11Aug17 
module load mpi
CASE_PATH=$WORK/$[PARAMPY-STUDYNAME]/$[PARAMPY-CASENAME]
cp -R $CASE_PATH/* $TMPDIR
mpirun -np 24 lammps -log output/log.lammps -in exec/in.lammps
cp -R $TMPDIR/output $CASE_PATH/output
