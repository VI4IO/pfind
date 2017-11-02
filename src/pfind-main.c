#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>

#include "pfind-options.h"

int pfind_rank;

int pfind_find(void * p){

}

int main(int argc, char ** argv){
  // output help with --help to enable running without mpiexec
  for(int i=0; i < argc; i++){
    if (strcmp(argv[i], "--help") == 0){
      argv[i][0] = 0;
      pfind_rank = 0;
      pfind_parse_args(argc, argv, 1);
      exit(0);
    }
  }

  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, & pfind_rank);

  pfind_options_t * options = pfind_parse_args(argc, argv, 0);

  MPI_Barrier(MPI_COMM_WORLD);

  pfind_find(options);
  MPI_Finalize();
  return 0;
}
