#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <assert.h>
#include <inttypes.h>

#include "pfind-options.h"

int pfind_rank;
int pfind_size;

static void print_result(pfind_options_t * options, pfind_find_results_t * find, char * prefix){
  if(options->print_rates){
    printf("[%s] rate: %.3f kiops time: %.1fs", prefix, find->rate / 1000, find->runtime);
  }else{
    printf("[%s]", prefix);
  }
  printf(" found: %ld (scanned %ld files, scanned dirents: %ld, unknown dirents: %ld", find->found_files, find->total_files, find->checked_dirents, find->unknown_file);
  if(options->verbosity > 0){
    printf(", job steal msgs received: %"PRIu64", work items send: %"PRIu64", job steal msgs send: %"PRIu64", work items stolen: %"PRIu64", time spend in job stealing: %.3fs, number of completion tokens send: %"PRIu64,
    find->monitor.job_steal_inbound,
    find->monitor.work_send,
    find->monitor.job_steal_tries,
    find->monitor.work_stolen,
    find->monitor.job_steal_mpitime_us / 1000000.0,
    find->monitor.completion_tokens_send
    );
  }
  printf(")\n");
}

int main(int argc, char ** argv){
  // output help with --help to enable running without mpiexec
  for(int i=0; i < argc; i++){
    if (strcmp(argv[i], "--help") == 0){
      argv[i][0] = 0;
      pfind_rank = 0;
      pfind_parse_args(argc, argv, 1, MPI_COMM_SELF);
      exit(0);
    }
  }

  MPI_Init(&argc, &argv);

  pfind_options_t * options = pfind_parse_args(argc, argv, 0, MPI_COMM_WORLD);

  pfind_find_results_t * find = pfind_find(options);

  if (options->print_by_process){
    char rank[15];
    sprintf(rank, "%d", pfind_rank);
    print_result(options, find, rank);
  }

  pfind_find_results_t * aggregated = pfind_aggregrate_results(find);
  if(pfind_rank == 0){
    print_result(options, aggregated, "DONE");
    printf("MATCHED %ld/%ld\n", aggregated->found_files, aggregated->total_files);
  }

  free(find);

  MPI_Finalize();
  return 0;
}
