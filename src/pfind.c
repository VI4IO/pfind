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
#include <assert.h>

#include "pfind-options.h"

#ifndef PATH_MAX
#define PATH_MAX 4096
#warning "Could not detect PATH_MAX"
#endif

#define MSG_COMPLETE 777
#define MSG_JOB_STEAL 800
#define MSG_JOB_STEAL_RESPONSE 820

//#define debug printf
#define debug(...)

// this version is based on mpistat
// parallel recursive find

// globals
static char start_dir[PATH_MAX]; // absolute path of start directory
static int start_dir_length;
static int msg_type_flag = 0;

static pfind_options_t * opt;
static pfind_find_results_t * res = NULL;

typedef struct{
  uint64_t    ctime_min;
  double      stonewall_endtime;
  FILE *      logfile;
  int         needs_stat;
} pfind_runtime_options_t;

static pfind_runtime_options_t runtime;
static void find_do_readdir(char *path);
static void find_do_lstat(char *path);

typedef struct {
  char type; // 'd' for directory
  char name[PATH_MAX];
} work_t;

static DIR * open_dir = NULL;
static char open_dir_name[PATH_MAX];

// queue of work
static work_t * work;
// amount of pending_work
static int pending_work = 0;

static int enqueue_work(char typ, char * path, char * entry){
  int ret = pending_work >= opt->queue_length;
  if (ret){
    return 1;
  }

  work[pending_work].type = typ;
  sprintf(work[pending_work].name, "%s/%s", path, entry);
  //printf("Queuing: %s\n", work[pending_work].name);
  pending_work++;
  return 0;
}

static void find_process_one_item(){
  if(open_dir){
    find_do_readdir(open_dir_name);
    return;
  }

  pending_work--;
  // opt->queue_length
  work_t * cur = & work[pending_work];
  if( cur->type == 'd'){
    find_do_readdir(cur->name);
  }else{
    find_do_lstat(cur->name);
  }
}

#define CHECK_MPI if(ret != MPI_SUCCESS){ printf("ERROR in %d\n", __LINE__); exit (1); }

pfind_find_results_t * pfind_find(pfind_options_t * lopt){
  opt = lopt;
  memset(& runtime, 0, sizeof(pfind_runtime_options_t));
  int ret;

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
    runtime.logfile = fopen(outfile, "w");

    if(runtime.logfile == NULL){
      pfind_abort("Could not open output logfile!\n");
    }
  }else{
    runtime.logfile = stdout;
  }

  // allocate sufficient large buffer size, we may receive up to size unexpected "stealing" calls
  int bsend_size = (pfind_size + 1) * MPI_BSEND_OVERHEAD;
  char * bsend_buf = malloc( bsend_size );
  MPI_Buffer_attach( bsend_buf, bsend_size );

  if(opt->timestamp_file || opt->size != UINT64_MAX){
    runtime.needs_stat = 1;
  }
  //ior_aiori_t * backend = aiori_select(opt->backend_name);
  work = malloc(sizeof(work_t) * (opt->queue_length + 1 ));
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

  // startup, load current directory
  if(pfind_rank == 0){
    work[0].type = 'd';
    work[0].name[0] = 0;
    pending_work++;
  }

  int have_finalize_token = (pfind_rank == 0);
  // a token indicating that I'm msg_type and other's to the left
  int have_processed_work_after_token = 1;
  int phase = 0; // 1 == potential cleanup; 2 == stop query; 3 == terminate

  while(! msg_type_flag){
    //usleep(100000);
    debug("[%d] processing: %d [%d, %d, phase: %d]\n", pfind_rank, pending_work, have_finalize_token, have_processed_work_after_token, phase);
    // do we have more work?
    if(pending_work > 0){
      if (opt->stonewall_timer && MPI_Wtime() >= runtime.stonewall_endtime ){
        if(opt->verbosity > 1){
          printf("Hit stonewall at %.2fs\n", MPI_Wtime());
        }
        pending_work = 0;
      }else{
        find_process_one_item();
      }
    }

    int has_msg;
    // try to retrieve token from a neighboring process
    int left_neighbor = pfind_rank == 0 ? pfind_size - 1 : (pfind_rank - 1);
    ret = MPI_Iprobe(left_neighbor, MSG_COMPLETE, MPI_COMM_WORLD, & has_msg, MPI_STATUS_IGNORE);
    CHECK_MPI
    if(has_msg){
      ret = MPI_Recv(& phase, 1, MPI_INT, left_neighbor, MSG_COMPLETE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      CHECK_MPI
      have_finalize_token = 1;
      debug("[%d] recvd finalize token\n", pfind_rank);
    }

    // we msg_type our last piece of work
    if (pending_work == 0){
      // Exit condition requesting_rank
      if (have_finalize_token){
        if (have_processed_work_after_token){
          phase = 0;
        }else if(pfind_rank == 0){
          phase++;
        }
        if( pfind_size == 1){
          break;
        }
        // send the finalize token to the right process
        debug("[%d] forwarding token\n", pfind_rank);
        ret = MPI_Bsend(& phase, 1, MPI_INT, (pfind_rank + 1) % pfind_size, MSG_COMPLETE, MPI_COMM_WORLD);
        CHECK_MPI
        // we received the finalization token
        if (phase == 3){
          break;
        }
        have_processed_work_after_token = 0;
        have_finalize_token = 0;
      }
    }

    MPI_Status wait_status;
    // check for job-stealing request
    ret = MPI_Iprobe(MPI_ANY_SOURCE, MSG_JOB_STEAL, MPI_COMM_WORLD, & has_msg, & wait_status);
    CHECK_MPI
    if(has_msg){
      int requesting_rank = wait_status.MPI_SOURCE;
      ret = MPI_Recv(NULL, 0, MPI_INT, requesting_rank, MSG_JOB_STEAL, MPI_COMM_WORLD, & wait_status);
      CHECK_MPI
      debug("[%d] msg ready from %d !\n", pfind_rank, requesting_rank);
      int work_to_give = pending_work / 2;
      pending_work -= work_to_give;
      ret = MPI_Send(& work[pending_work], sizeof(work_t)*work_to_give, MPI_CHAR, requesting_rank, MSG_JOB_STEAL_RESPONSE, MPI_COMM_WORLD);
      CHECK_MPI
    }

    int work_received = 0;
    int steal_neighbor = pfind_rank;
    if (pending_work == 0 && phase < 2){
      // send request for job stealing
      steal_neighbor = rand() % pfind_size;
      if(steal_neighbor != pfind_rank){
        debug("[%d] msg attempting to steal from %d\n", pfind_rank, steal_neighbor);
        ret = MPI_Bsend(NULL, 0, MPI_INT, steal_neighbor, MSG_JOB_STEAL, MPI_COMM_WORLD);
        CHECK_MPI
        // check for any pending work stealing request, too.
        while(1){
          ret = MPI_Iprobe(steal_neighbor, MSG_JOB_STEAL_RESPONSE, MPI_COMM_WORLD, & has_msg, & wait_status);
          CHECK_MPI
          if(has_msg){
            MPI_Get_count(& wait_status, MPI_CHAR, & work_received);

            ret = MPI_Recv(work, work_received, MPI_CHAR, steal_neighbor, MSG_JOB_STEAL_RESPONSE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            CHECK_MPI

            work_received /= sizeof(work_t);
            break;
          }
          ret = MPI_Iprobe(MPI_ANY_SOURCE, MSG_JOB_STEAL, MPI_COMM_WORLD, & has_msg, MPI_STATUS_IGNORE);
          CHECK_MPI
          if(has_msg){
            ret = MPI_Recv(NULL, 0, MPI_INT, MPI_ANY_SOURCE, MSG_JOB_STEAL, MPI_COMM_WORLD, & wait_status);
            CHECK_MPI
            int requesting_rank = wait_status.MPI_SOURCE;
            debug("[%d] msg ready from %d, but no work pending!\n", pfind_rank, requesting_rank);
            ret = MPI_Send(NULL, 0, MPI_CHAR, requesting_rank, MSG_JOB_STEAL_RESPONSE, MPI_COMM_WORLD);
            CHECK_MPI
          }
        }
        debug("[%d] stole %d \n", pfind_rank, work_received);
        pending_work = work_received;
        if (work_received > 0){
          have_processed_work_after_token = 1;
        }
      }
    }
  }
  debug("[%d] ended\n", pfind_rank);

  free(work);
  MPI_Buffer_detach(bsend_buf, & bsend_size);

  double end = MPI_Wtime();
  res->runtime = end - start;

  if(runtime.logfile != stdout){
    fclose(runtime.logfile);
  }

  return res;
}

pfind_find_results_t * pfind_aggregrate_results(pfind_find_results_t * local){
  pfind_find_results_t * res = malloc(sizeof(pfind_find_results_t));
  if(! res){
    pfind_abort("No memory");
  }
  memcpy(res, local, sizeof(*res));

  MPI_Reduce(pfind_rank == 0 ? MPI_IN_PLACE : & res->errors, & res->errors, 5, MPI_LONG_LONG_INT, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(pfind_rank == 0 ? MPI_IN_PLACE : & res->runtime, & res->runtime, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

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
    if(! opt->just_count){
      fprintf(runtime.logfile, "%s\n", path);
    }
    res->found_files++;
}

static void find_do_lstat(char *path) {
  char dir[PATH_MAX];
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
    fprintf(runtime.logfile, "Error stating file: %s\n", dir);
  }
}

static void find_do_readdir(char *path) {
    char dir[PATH_MAX];
    sprintf(dir, "%s%s", start_dir, path);
    path = & dir[start_dir_length];

    DIR *d;
    if( open_dir ){
      d = open_dir;
    }else{
      d = opendir(dir);
      if (!d) {
          if(opt->verbosity > 1){
            fprintf(runtime.logfile, "Cannot open '%s': %s\n", dir, strerror (errno));
          }
          res->errors++;
          return;
      }
    }
    int fd = dirfd(d);
    while (1) {
        //printf("find_do_readdir %s - %s\n", dir, path);

        struct dirent *entry;
        entry = readdir(d);
        if (opt->stonewall_timer && MPI_Wtime() >= runtime.stonewall_endtime ){
          if(opt->verbosity > 1){
            printf("Hit stonewall at %.2fs\n", MPI_Wtime());
          }
          break;
        }
        if (entry == 0) {
            break;
        }
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        res->checked_dirents++;

        char typ = find_file_type(entry->d_type);
        if (typ == 'u'){
          res->unknown_file++;
          // sometimes the filetype is not provided by readdir.
          struct stat buf;
          if (fstatat(fd, entry->d_name, & buf, 0 )) {
            res->errors++;
            if(opt->verbosity > 1){
              fprintf(runtime.logfile, "Error stating file: %s\n", dir);
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
            if(! opt->just_count){
              fprintf(runtime.logfile, "%s/%s\n", dir, entry->d_name);
            }
            continue;
          }
        }
        if(enqueue_work(typ, path, entry->d_name)){
          if(open_dir == NULL){
            strcpy(open_dir_name, path);
            open_dir = d;
          }
          char cur_path[PATH_MAX];
          sprintf(cur_path, "%s/%s", path, entry->d_name);

          if(typ == 'd'){
            printf("[%d] WARNING, dropped processing of the directory %s as the queue is full\n", pfind_rank, cur_path);
          }else{
            find_do_lstat(cur_path);
          }
          return;
        }
    }
    closedir(d);
    open_dir = NULL;
}
