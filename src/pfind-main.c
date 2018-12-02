#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>

#include "pfind-options.h"

int pfind_rank;
int pfind_size;

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
  MPI_Comm_size(MPI_COMM_WORLD, & pfind_size);

  pfind_options_t * options = pfind_parse_args(argc, argv, 0);

  pfind_find_results_t * find = pfind_find(options);
  if(pfind_rank == 0){
    if(options->print_rates){
      printf("[DONE] rate: %.3f kiops time: %.1fs err: %ld found: %ld (scanned %ld files)\n",  find->rate / 1000, find->runtime, find->errors, find->found_files, find->total_files);
    }else{
      printf("[DONE] found: %ld (scanned %ld files, err: %ld, unknown_during_dirent:%ld)\n", find->found_files, find->total_files, find->errors, find->unknown_file);
    }
    printf("MATCHED %ld/%ld\n", find->found_files, find->total_files);
  }

  MPI_Finalize();
  return 0;
}
