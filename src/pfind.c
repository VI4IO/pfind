#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <string.h>
#include <errno.h>
#include <dirent.h>
#include <limits.h>
#include <sys/stat.h>
#include <mpi.h>

#include <libcircle.h>
#include "pfind-options.h"

// this version is based on mpistat
// parallel recursive find

// globals
static char start_dir[8192]; // absolute path of start directory
static char item_buf[8192]; // buffer to construct type / path combos for queue items

static pfind_options_t * opt;
static pfind_find_results_t * res = NULL;

static struct pfind_runtime_options_t{
  uint64_t    ctime_min;
  double      stonewall_endtime;
  FILE *      logfile;
} runtime;

static void find_create_work(CIRCLE_handle *handle);
static void find_process_work(CIRCLE_handle *handle);


pfind_find_results_t * pfind_find(pfind_options_t * lopt){
  opt = lopt;

  if(opt->timestamp_file){
    static struct stat timer_file;
    if(lstat(opt->timestamp_file, & timer_file) != 0) {
      pfind_abort("Could not read timestamp file!");
    }
    runtime.ctime_min = timer_file.st_ctime;
  }

  if(opt->results_dir && ! opt->just_count){
    char outfile[10240];
    sprintf(outfile, "%s/%d.txt", opt->results_dir, pfind_rank);
    mkdir(opt->results_dir, S_IRWXU);
    runtime.logfile = fopen(outfile, "w");

    if(! runtime.logfile){
      pfind_abort("Could not open output logfile!\n");
    }
  }else{
    runtime.logfile = NULL;
  }


  int argc = 1;
  char * argv[] = {"test"};
	CIRCLE_init(argc, argv, CIRCLE_SPLIT_RANDOM);
  CIRCLE_enable_logging(CIRCLE_LOG_FATAL);

  //ior_aiori_t * backend = aiori_select(opt->backend_name);
  double start = MPI_Wtime();
  char * err = realpath(opt->workdir, start_dir);
  res = malloc(sizeof(pfind_find_results_t));
  memset(res, 0, sizeof(*res));
  if(opt->stonewall_timer != 0){
    runtime.stonewall_endtime = MPI_Wtime() + opt->stonewall_timer;
  }else{
    runtime.stonewall_endtime = 0;
  }

  DIR * sd=opendir(start_dir);
  if (err == NULL || ! sd) {
      fprintf (stderr, "Cannot open directory '%s': %s\n", start_dir, strerror (errno));
      exit (EXIT_FAILURE);
  }
  memset(item_buf, 0, sizeof(item_buf));
  sprintf(item_buf, "%c%s", 'd', start_dir);

	// set the create work callback
  CIRCLE_cb_create(& find_create_work);

	// set the process work callback
	CIRCLE_cb_process(& find_process_work);

	// enter the processing loop
	CIRCLE_begin();

	// wait for all processing to finish and then clean up
	CIRCLE_finalize();

  double end = MPI_Wtime();
  res->runtime = end - start;

  if(runtime.logfile){
    fclose(runtime.logfile);
  }

  double runtime = res->runtime;
  long long found = res->found_files;
  long long total_files = res->total_files;
  MPI_Reduce(& runtime, & res->runtime, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
  MPI_Reduce(& found, & res->found_files, 1, MPI_LONG_LONG_INT, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(& total_files, & res->total_files, 1, MPI_LONG_LONG_INT, MPI_SUM, 0, MPI_COMM_WORLD);

  res->rate = res->total_files / res->runtime;

  return res;
}

static char  find_file_type(unsigned char c) {
    switch (c) {
        case DT_BLK :
            return 'b';
        case DT_CHR :
            return 'c';
        case DT_DIR :
            return 'd';
        case DT_FIFO :
            return 'F';
        case DT_LNK :
            return 'l';
        case DT_REG :
            return 'f';
        case DT_SOCK :
            return 's';
        default :
            return 'u';
    }
}

static void check_buf(struct stat buf, char * path){
    // compare values
    if(opt->timestamp_file){
      if( (uint64_t) buf.st_ctime < runtime.ctime_min ){
        if(opt->verbosity >= 2 && runtime.logfile){
          fprintf(runtime.logfile, "Timestamp too small: %s\n", path);
        }
        return;
      }
    }
    if(opt->size != UINT64_MAX){
      uint64_t size = (uint64_t) buf.st_size;
      if(size != opt->size){
        if(opt->verbosity >= 2 && runtime.logfile){
          fprintf(runtime.logfile, "Size does not match: %s has %zu bytes\n", path, (size_t) buf.st_size);
        }
        return;
      }
    }
    if(runtime.logfile && ! opt->just_count){
      fprintf(runtime.logfile, "%s\n", path + 1);
    }
    res->found_files++;
}

static void find_do_lstat(char *path) {
  struct stat buf;
  // filename comparison has been done already
  if(opt->verbosity >= 2 && runtime.logfile){
    fprintf(runtime.logfile, "STAT: %s\n", path);
  }

  if (lstat(path+1, & buf) == 0) {
    check_buf(buf, path);
  } else {
    res->errors++;
    if(runtime.logfile){
      fprintf(runtime.logfile, "Error stating file: %s\n", path);
    }
  }
}

static void find_do_readdir(char *path, CIRCLE_handle *handle) {
    int path_len = strlen(path+1);
    DIR *d = opendir(path+1);
    if (!d) {
        if(runtime.logfile){
          fprintf (runtime.logfile, "Cannot open '%s': %s\n", path+1, strerror (errno));
        }
        return;
    }
    int fd = dirfd(d);
    while (1) {
        struct dirent *entry;
        entry = readdir(d);
        if (opt->stonewall_timer && MPI_Wtime() >= runtime.stonewall_endtime ){
          if(opt->verbosity > 1 && runtime.logfile){
            fprintf (runtime.logfile, "Hit stonewall at %.2fs\n", MPI_Wtime());
          }
          break;
        }
        if (entry==0) {
            break;
        }
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        char typ = find_file_type(entry->d_type);
        if (typ == 'u'){
          // sometimes the filetype is not provided by readdir.

          struct stat buf;
          if (fstatat(fd, entry->d_name, & buf, 0 )) {
            res->errors++;
            if(runtime.logfile){
              fprintf(runtime.logfile, "Error stating file: %s\n", path);
            }
            continue;
          }
          typ = S_ISDIR(buf.st_mode) ? 'd' : 'f';

          if(typ == 'f'){ // since we have done the stat already, it would be a waste to do it again
            res->total_files++;
            // compare values
            if(opt->name != NULL && strstr(entry->d_name, opt->name) == NULL){
              if(opt->verbosity >= 2 && runtime.logfile){
                fprintf(runtime.logfile, "Name does not match: %s\n", entry->d_name);
              }
              continue;
            }
            check_buf(buf, entry->d_name);
            continue;
          }
        }else if (typ != 'd'){
          res->total_files++;
          // compare file name
          if(opt->name != NULL && strstr(entry->d_name, opt->name) == NULL){
            if(opt->verbosity >= 2 && runtime.logfile){
              fprintf(runtime.logfile, "Name does not match: %s\n", entry->d_name);
            }
            continue;
          }
        }
        char *tmp=(char*) malloc(path_len+strlen(entry->d_name)+3);
        *tmp = typ;
        strcpy(tmp+1, path+1);
        *(tmp+path_len+1)='/';
        strcpy(tmp + path_len+2, entry->d_name);
        handle->enqueue(tmp);
    }
    closedir(d);
}

// create work callback
// this is called once at the start on pfind_rank 0
// use to seed pfind_rank 0 with the initial dir to start
// searching from
static void find_create_work(CIRCLE_handle *handle) {
    handle->enqueue(item_buf);
}

// process work callback
static void find_process_work(CIRCLE_handle *handle)
{
    // dequeue the next item
    handle->dequeue(item_buf);
    if (*item_buf == 'd') {
      find_do_readdir(item_buf, handle);
    }else{
      find_do_lstat(item_buf);
    }
}
