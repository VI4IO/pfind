# Parallel find using MPI

This command is a drop-in replacement for find, implemented for using parallel access and MPI.

## Usage:

     # Scanning the homedir with a stonewall timer of 10 seconds, checking for files newer than test/0.txt with name *01*, outputing rates, not outputing file names
     $ mpiexec -n 2 ./pfind /home -s 10 -newer test/0.txt -name 01 -D rates -C


To test with single dir parallel access:

    $ mkdir testdir
    $ for I in $(seq 1 100) ; do touch testdir/$I ; done
    $ mpiexec -np 5 ./pfind testdir/ -C -M 10 -P # regular access should work but show that one process does all
    $ mpiexec -np 5 ./pfind testdir/ -C -M 10 -H 2 -P # with parallel access
