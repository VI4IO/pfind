# Parallel find using MPI

## Usage:

     # Scanning the homedir with a stonewall timer of 10 seconds, checking for files newer than test/0.txt with name *01*, outputing rates, not outputing file names
     mpiexec -n 2 ./pfind /home -s 10 -newer test/0.txt -name 01 -D rates -C

