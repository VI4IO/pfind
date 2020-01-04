# Parallel find using MPI

This command is a drop-in replacement for find, implemented for using parallel access and MPI.

## Usage:

     # Scanning the homedir with a stonewall timer of 10 seconds, checking for files newer than test/0.txt with name *01*, outputing rates, not outputing file names
     $ mpiexec -n 2 ./pfind /home -s 10 -newer test/0.txt -name 01 -D rates -C

## Compatibility for Single Directory Parallel Access

The tool provides parallel access to a single directory.
However, this feature depends on the distribution of the "cookie" returned by telldir().
Depending on the system, it may work.

### Compatibility

The option -H 1 works with:
   * Lustre 2.5 (and later)
   * NFSv3

To test if single dir parallel access works, run:
    $ mkdir testdir
    $ for I in $(seq 1 100) ; do touch testdir/$I ; done
    $ mpiexec -np 5 ./pfind testdir/ -C -M 10 -P # regular access should work but show that one process does all
    $ mpiexec -np 5 ./pfind testdir/ -C -M 10 -H 2 -P # with parallel access

In test/pfind-behavior.c a program is provided that should help to debug the issue.
