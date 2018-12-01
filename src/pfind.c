#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <string.h>
#include <errno.h>
#include <dirent.h>
#include <limits.h>
#include <sys/stat.h>
#include <mpi.h>
#include <fcntl.h>
#include <unistd.h>

#include "pfind-options.h"

// this version is based on mpistat
// parallel recursive find

// globals
static char start_dir[8192]; // absolute path of start directory
static int start_dir_length;

static pfind_options_t * opt;
static pfind_find_results_t * res = NULL;

typedef struct{
  uint64_t    ctime_min;
  double      stonewall_endtime;
  int         logfile;
  int         needs_stat;
} pfind_runtime_options_t;

static pfind_runtime_options_t runtime;
static void find_do_readdir(char *path);
static void find_do_lstat(char *path);

// process work callback
static void find_process_work()
{
  MPI_Status status;
  char item_buf[8192]; // buffer to construct type / path combos for queue items
  int ret;
  while(1){
    ret = MPI_Recv(item_buf, 8192, MPI_CHAR, MPI_ANY_SOURCE, 4711, MPI_COMM_WORLD, & status);
    if(opt->verbosity >= 2){
      printf("Recvd %d: %s\n", pfind_rank, item_buf);
    }

    if (*item_buf == '0') {
      // stop message
      if( pfind_rank == 0){
        for(int i=1; i < pfind_size; i++){
          MPI_Send("0", 1, MPI_CHAR, i, 4711, MPI_COMM_WORLD);
        }
      }
      return;
    }else if (*item_buf == 'd') {
      find_do_readdir(item_buf + 1);
    }else{
      find_do_lstat(item_buf + 1);
    }
  }
}

static void pfind_send_random(int is_dir, char * dir, char * entry){
  char item_buf[8192];
  MPI_Request request;
  int ret;
  int rnd = rand() % pfind_size;
  sprintf(item_buf, "%c%s/%s", is_dir ? 'd' : 'f', dir, entry);
  if(opt->verbosity >= 2){
    printf("%d->%d send: %s\n", pfind_rank, rnd, item_buf);
  }
  ret = MPI_Isend(item_buf, strlen(item_buf) +1, MPI_CHAR, rnd, 4711, MPI_COMM_WORLD, & request);

}

pfind_find_results_t * pfind_find(pfind_options_t * lopt){
  opt = lopt;
  memset(& runtime, 0, sizeof(pfind_runtime_options_t));

  if(opt->timestamp_file){
    if(pfind_rank==0) {
        static struct stat timer_file;
        if(lstat(opt->timestamp_file, & timer_file) != 0) {
          pfind_abort("Could not read timestamp file!");
        }
        runtime.ctime_min = timer_file.st_ctime;
    }
    MPI_Bcast(&runtime.ctime_min, 1, MPI_INT, 0, MPI_COMM_WORLD);
  }

  if(opt->results_dir && ! opt->just_count){
    char outfile[10240];
    sprintf(outfile, "%s/%d.txt", opt->results_dir, pfind_rank);
    mkdir(opt->results_dir, S_IRWXU);
    runtime.logfile = open(outfile, O_CREAT|O_WRONLY|O_TRUNC, S_IWUSR | S_IRUSR);

    if(runtime.logfile == -1){
      pfind_abort("Could not open output logfile!\n");
    }
  }else{
    runtime.logfile = 2;
  }

  if(opt->timestamp_file || opt->size != UINT64_MAX){
    runtime.needs_stat = 1;
  }

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

  DIR * sd = opendir(start_dir);
  if (err == NULL || ! sd) {
      fprintf (stderr, "Cannot open directory '%s': %s\n", start_dir, strerror (errno));
      exit (EXIT_FAILURE);
  }
  start_dir_length = strlen(start_dir);

  // startup
  MPI_Bsend("d/", 3, MPI_CHAR, 0, 4711, MPI_COMM_WORLD);
  find_process_work();

  double end = MPI_Wtime();
  res->runtime = end - start;

  if(runtime.logfile != 2){
    close(runtime.logfile);
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
        if(opt->verbosity >= 2){
          printf("Timestamp too small: %s\n", path);
        }
        return;
      }
    }
    if(opt->size != UINT64_MAX){
      uint64_t size = (uint64_t) buf.st_size;
      if(size != opt->size){
        if(opt->verbosity >= 2){
          printf("Size does not match: %s has %zu bytes\n", path, (size_t) buf.st_size);
        }
        return;
      }
    }
    if(runtime.logfile && ! opt->just_count){
      printf("%s\n", path);
    }
    res->found_files++;
}

static void find_do_lstat(char *path) {
  char dir[8192];
  sprintf(dir, "%s/%s", start_dir, path);
  struct stat buf;
  // filename comparison has been done already
  if(opt->verbosity >= 2){
    printf("STAT: %s\n", dir);
  }

  if (lstat(dir, & buf) == 0) {
    check_buf(buf, dir);
  } else {
    res->errors++;
    if(runtime.logfile){
      printf("Error stating file: %s\n", dir);
    }
  }
}

static void find_do_readdir(char *path) {
    char dir[8192];
    sprintf(dir, "%s%s", start_dir, path);
    DIR *d = opendir(dir);
    if (!d) {
        if(runtime.logfile){
          printf("Cannot open '%s': %s\n", dir, strerror (errno));
        }
        return;
    }
    int fd = dirfd(d);
    while (1) {
        struct dirent *entry;
        entry = readdir(d);
        if (opt->stonewall_timer && MPI_Wtime() >= runtime.stonewall_endtime ){
          if(opt->verbosity > 1){
            printf("Hit stonewall at %.2fs\n", MPI_Wtime());
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
              printf("Error stating file: %s\n", dir);
            }
            continue;
          }
          typ = S_ISDIR(buf.st_mode) ? 'd' : 'f';

          if(typ == 'f'){ // since we have done the stat already, it would be a waste to do it again
            res->total_files++;
            // compare values
            if(opt->name_pattern && regexec(& opt->name_regex, entry->d_name, 0, NULL, 0) ){
              if(opt->verbosity >= 2){
                printf("Name does not match: %s\n", entry->d_name);
              }
              continue;
            }
            check_buf(buf, entry->d_name);
            continue;
          }
        }else if (typ != 'd'){
          res->total_files++;
          // compare file name
          if(opt->name_pattern && regexec(& opt->name_regex, entry->d_name, 0, NULL, 0) ){
            if(opt->verbosity >= 2){
              printf("Name does not match: %s\n", entry->d_name);
            }
            continue;
          }
          if(! runtime.needs_stat){
            // optimization to skip stat
            res->found_files++;
            if(runtime.logfile && ! opt->just_count){
              printf("%s/%s\n", dir, entry->d_name);
            }
            continue;
          }
        }
        pfind_send_random(typ == 'd', path, entry->d_name);
    }
    closedir(d);
}
