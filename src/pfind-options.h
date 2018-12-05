#ifndef PFIND_OPTIONS_H
#define PFIND_OPTIONS_H

#include <stdint.h>
#include <regex.h>

extern int pfind_rank;
extern int pfind_size;

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
  int queue_length;

  int verbosity;
} pfind_options_t;


typedef struct{
  uint64_t errors;
  uint64_t unknown_file;

  uint64_t found_files;
  uint64_t total_files;

  uint64_t checked_dirents;

  double rate;
  double runtime;
} pfind_find_results_t;

pfind_find_results_t * pfind_find(pfind_options_t * opt);
pfind_find_results_t * pfind_aggregrate_results(pfind_find_results_t * local);
pfind_options_t * pfind_parse_args(int argc, char ** argv, int force_print_help);
void pfind_abort(char * str);

#ifndef UINT64_MAX
#define UINT64_MAX ((uint64_t) -1)
#endif

#endif
