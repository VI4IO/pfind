#ifndef PFIND_OPTIONS_H
#define PFIND_OPTIONS_H

#include <stdint.h>
#include <regex.h>
#include <mpi.h>

extern int pfind_rank;
extern int pfind_size;
extern MPI_Comm pfind_com;

typedef struct {
  // https://www.gnu.org/software/findutils/manual/html_mono/find.html
  char * workdir;
  int just_count;
  int print_by_process;
  char * results_dir;
  int stonewall_timer;
  int print_rates;

  char * timestamp_file;
  char * name_pattern;
  regex_t name_regex;
  uint64_t size;
  // optimizing parameters
  int queue_length;
  int max_entries_per_iter;
  int steal_from_next; // if true, then steal from the next process
  int parallel_single_dir_access; // if 1, use hashing to parallelize single directory access, if 2 sequential increment

  int verbosity;
} pfind_options_t;

typedef struct {
  uint64_t job_steal_inbound;
  uint64_t work_send;
  uint64_t job_steal_tries;
  uint64_t work_stolen;
  uint64_t job_steal_mpitime_us; // microseconds spend in job steal attempts (MPI)
  uint64_t completion_tokens_send;
} pfind_monitoring_t;

typedef struct{
  uint64_t errors;
  uint64_t unknown_file;

  uint64_t found_files;
  uint64_t total_files;

  uint64_t checked_dirents;

  double rate;
  double runtime;

  pfind_monitoring_t monitor;
  MPI_Comm com;
} pfind_find_results_t;

pfind_find_results_t * pfind_find(pfind_options_t * opt);
pfind_find_results_t * pfind_aggregrate_results(pfind_find_results_t * local);
pfind_options_t * pfind_parse_args(int argc, char ** argv, int force_print_help, MPI_Comm com);
void pfind_abort(char * str);

#ifndef UINT64_MAX
#define UINT64_MAX ((uint64_t) -1)
#endif

#endif
