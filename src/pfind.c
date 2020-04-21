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
#include <sys/time.h>

#ifdef LZ4
#include <lz4.h>
#endif

#include "pfind-options.h"

#ifndef PATH_MAX
#define PATH_MAX 4096
#warning "Could not detect PATH_MAX"
#endif

#define MSG_COMPLETE 777
#define MSG_JOB_STEAL 800
#define MSG_JOB_STEAL_RESPONSE 820


int pfind_rank;
int pfind_size;
MPI_Comm pfind_com;


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
static int find_do_readdir(char *path, uint64_t dir_start, uint64_t dir_end);
static void find_do_lstat(char *path);

typedef struct {
  char name[PATH_MAX];
  uint64_t dir_start; // start position to read a directory
  uint64_t dir_end;   // end position to read a directory
  char type; // 'd' for directory
} work_t;

typedef struct { // a directory work item, that is fetched in excess of processing capabilities
  char name[PATH_MAX];
} work_dir_t;

typedef struct{
  work_dir_t * dirs;
  int capacity;
  int size;
} work_excess_dir_t;

typedef struct{
  DIR * dir;
  char name[PATH_MAX];
  uint64_t pos_cur;
  uint64_t pos_end;
} currently_processed_dir_t;

static currently_processed_dir_t current_dir = {NULL};

// queue of work
static work_t * work;

// additional directories
work_excess_dir_t excess_dirs = {NULL, 0, 0};

// amount of pending_work
static int pending_work = 0;

static void* smalloc(size_t size){
  void * p = malloc(size);
  memset(p, 0, size);
  if(p == NULL){
    printf("Cannot allocate %zd bytes of memory\nAborting\n", size);
    exit(1);
  }
  return p;
}

static void pfind_timer_start(struct timeval * timer){
  if (gettimeofday(timer, (struct timezone *)NULL) != 0){
    printf("Error using gettimeofday()");
    exit(1);
  }
}

static uint64_t pfind_timer_diff_usec(struct timeval * start){
        struct timeval timer;
        pfind_timer_start(& timer);
        uint64_t time = (timer.tv_sec - start->tv_sec) * 1000000 + (timer.tv_usec - start->tv_usec);
        return time;
}

static void enqueue_dir_excess(char * path, char * entry){

  if(excess_dirs.capacity == excess_dirs.size){
    int newcapacity = excess_dirs.size * 2 + 5;
    excess_dirs.dirs = realloc(excess_dirs.dirs, newcapacity * sizeof(work_dir_t));
    excess_dirs.capacity = newcapacity;
  }
  sprintf(excess_dirs.dirs[excess_dirs.size].name, "%s/%s", path, entry);

  excess_dirs.size++;
}

static int enqueue_work(char typ, char * path, char * entry){
  int ret = pending_work >= opt->queue_length;
  if (ret){
    return 1;
  }

  work[pending_work].type = typ;
  work[pending_work].dir_end = 0;
  work[pending_work].dir_start = 0;
  sprintf(work[pending_work].name, "%s/%s", path, entry);
  //printf("Queuing: %s\n", work[pending_work].name);
  pending_work++;
  return 0;
}

static void find_process_one_item(){
  if(current_dir.dir){
    find_do_readdir(current_dir.name, current_dir.pos_cur, current_dir.pos_end);
    return;
  }

  // opt->queue_length
  if(excess_dirs.size > 0){
    excess_dirs.size--;
    find_do_readdir(excess_dirs.dirs[excess_dirs.size].name, 0, 0);
    return;
  }
  pending_work--;

  work_t * cur = & work[pending_work];
  if( cur->type == 'd'){
    find_do_readdir(cur->name, cur->dir_start, cur->dir_end);
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
          printf("Could not open: \"%s\", error: %s", opt->timestamp_file, strerror(errno));
          pfind_abort("\n");
        }
        runtime.ctime_min = timer_file.st_ctime;
    }
    MPI_Bcast(&runtime.ctime_min, 1, MPI_INT, 0, pfind_com);
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
  char * bsend_buf = smalloc( bsend_size );
  MPI_Buffer_attach( bsend_buf, bsend_size );

  if(opt->timestamp_file || opt->size != UINT64_MAX){
    runtime.needs_stat = 1;
  }
  //ior_aiori_t * backend = aiori_select(opt->backend_name);
  work = smalloc(sizeof(work_t) * (opt->queue_length + 1 ));
  double start = MPI_Wtime();
  char * err = realpath(opt->workdir, start_dir);
  res = smalloc(sizeof(pfind_find_results_t));
  memset(res, 0, sizeof(*res));

  #ifdef LZ4
    int max_compressed = LZ4_COMPRESSBOUND(opt->queue_length * sizeof(work_t) / 2);
    char * compress_buf = smalloc(max_compressed);
  #endif

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
    int have_completed = (pending_work == 0 && excess_dirs.size == 0 && current_dir.dir == NULL);
    debug("[%d] processing: %d [%d, %d, phase: %d]\n", pfind_rank, pending_work, have_finalize_token, have_processed_work_after_token, phase);
    // do we have more work?
    if(! have_completed){
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
    ret = MPI_Iprobe(left_neighbor, MSG_COMPLETE, pfind_com, & has_msg, MPI_STATUS_IGNORE);
    CHECK_MPI
    if(has_msg){
      ret = MPI_Recv(& phase, 1, MPI_INT, left_neighbor, MSG_COMPLETE, pfind_com, MPI_STATUS_IGNORE);
      CHECK_MPI
      have_finalize_token = 1;
      debug("[%d] recvd finalize token\n", pfind_rank);
    }

    // we msg_type our last piece of work
    if (have_completed){
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
        ret = MPI_Bsend(& phase, 1, MPI_INT, (pfind_rank + 1) % pfind_size, MSG_COMPLETE, pfind_com);
        CHECK_MPI
        res->monitor.completion_tokens_send++;
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
    has_msg = 1;
    while(has_msg){
      ret = MPI_Iprobe(MPI_ANY_SOURCE, MSG_JOB_STEAL, pfind_com, & has_msg, & wait_status);
      CHECK_MPI
      if(has_msg){
        int requesting_rank = wait_status.MPI_SOURCE;
        ret = MPI_Recv(NULL, 0, MPI_INT, requesting_rank, MSG_JOB_STEAL, pfind_com, & wait_status);
        CHECK_MPI
        debug("[%d] msg ready from %d !\n", pfind_rank, requesting_rank);

        int work_to_give = pending_work / 2;
        if(opt->parallel_single_dir_access && pending_work == 0 && current_dir.dir != NULL){
          // the current node keeps the current position but updates the end
          // the requester receives new position and updates position.
          uint64_t dir_new_end = 0;

          if(opt->parallel_single_dir_access == 1){
            uint64_t dir_span;
            dir_span = (current_dir.pos_end - current_dir.pos_cur) / 2;
            dir_new_end = current_dir.pos_cur + dir_span;
          }else if(opt->parallel_single_dir_access == 2){
            dir_new_end = current_dir.pos_cur + opt->max_entries_per_iter;
          }else{
            printf("Error INVALID option for parallel access!\n");
            exit(1);
          }

          strcpy(work[0].name, current_dir.name);
          work[0].type = 'd';
          work[0].dir_start = dir_new_end;
          work[0].dir_end = current_dir.pos_end;

          current_dir.pos_end = dir_new_end;
          work_to_give = 1;
          pending_work = 1;

          debug("[%d] splitting the directory into two equal-sized fragments: %llu-%llu and %llu-%llu\n", pfind_rank, (long long unsigned) (long long unsigned) work[0].dir_start, (long long unsigned) work[0].dir_end, (long long unsigned) current_dir.pos_cur, (long long unsigned)  current_dir.pos_end);
        }

        res->monitor.job_steal_inbound++;
        res->monitor.work_send += work_to_give;

        int datasize = sizeof(work_t)*work_to_give;
        pending_work -= work_to_give;

        #ifndef LZ4
          ret = MPI_Send(& work[pending_work], datasize, MPI_CHAR, requesting_rank, MSG_JOB_STEAL_RESPONSE, pfind_com);

        #else

        if(datasize > 0){
          int compressed_size;
          compressed_size = LZ4_compress_default((char*) & work[pending_work], compress_buf, datasize, max_compressed);
          ret = MPI_Send(compress_buf, compressed_size, MPI_CHAR, requesting_rank, MSG_JOB_STEAL_RESPONSE, pfind_com);
        }else{
          ret = MPI_Send(NULL, 0, MPI_CHAR, requesting_rank, MSG_JOB_STEAL_RESPONSE, pfind_com);
        }
        #endif
        CHECK_MPI

        // move excess directory queue into the regular queue
        if(excess_dirs.size > 0){

          int to_move = work_to_give < excess_dirs.size ? work_to_give : excess_dirs.size;
          int ex_pos = excess_dirs.size - 1;
          //printf("Move %d ex_pos: %d Move: %d\n", pending_work, ex_pos, to_move);
          for(int i = 0 ; i < to_move; i++, pending_work++, ex_pos--){
            work[pending_work].type = 'd';
            work[pending_work].dir_start = 0;
            strcpy(work[pending_work].name, excess_dirs.dirs[ex_pos].name);
          }
          excess_dirs.size -= to_move;
        }
      }
    }

    int work_received = 0;
    int steal_neighbor = pfind_rank;
    if (pending_work == 0 && phase < 2){
      // send request for job stealing
      if(opt->steal_from_next) {
        steal_neighbor = ((rand() % 2) && (pfind_rank > 0)) ? pfind_rank - 1 : rand() % pfind_size;
      }else{
        steal_neighbor = rand() % pfind_size;
      }
      if(steal_neighbor != pfind_rank){
        struct timeval t_start;
        pfind_timer_start(& t_start);
        res->monitor.job_steal_tries++;

        debug("[%d] msg attempting to steal from %d\n", pfind_rank, steal_neighbor);
        ret = MPI_Bsend(NULL, 0, MPI_INT, steal_neighbor, MSG_JOB_STEAL, pfind_com);
        CHECK_MPI
        // check for any pending work stealing request, too.
        while(1){
          ret = MPI_Iprobe(steal_neighbor, MSG_JOB_STEAL_RESPONSE, pfind_com, & has_msg, & wait_status);
          CHECK_MPI
          if(has_msg){
            MPI_Get_count(& wait_status, MPI_CHAR, & work_received);

            #ifndef LZ4
              ret = MPI_Recv(work, work_received, MPI_CHAR, steal_neighbor, MSG_JOB_STEAL_RESPONSE, pfind_com, MPI_STATUS_IGNORE);
            #else
            if(work_received > 0){
              ret = MPI_Recv(compress_buf, work_received, MPI_CHAR, steal_neighbor, MSG_JOB_STEAL_RESPONSE, pfind_com, MPI_STATUS_IGNORE);
              int decompressed_size = LZ4_decompress_safe (compress_buf, (char*)work, work_received, max_compressed);
              //printf("Before: %d after: %d\n", datasize, compressed_size);
              work_received = decompressed_size;
            }else{
              ret = MPI_Recv(work, work_received, MPI_CHAR, steal_neighbor, MSG_JOB_STEAL_RESPONSE, pfind_com, MPI_STATUS_IGNORE);
            }
            #endif
            CHECK_MPI

            work_received /= sizeof(work_t);
            break;
          }
          // job stealing request
          ret = MPI_Iprobe(MPI_ANY_SOURCE, MSG_JOB_STEAL, pfind_com, & has_msg, MPI_STATUS_IGNORE);
          CHECK_MPI
          if(has_msg){
            res->monitor.job_steal_inbound++;
            ret = MPI_Recv(NULL, 0, MPI_INT, MPI_ANY_SOURCE, MSG_JOB_STEAL, pfind_com, & wait_status);
            CHECK_MPI
            int requesting_rank = wait_status.MPI_SOURCE;
            debug("[%d] msg ready from %d, but no work pending!\n", pfind_rank, requesting_rank);
            ret = MPI_Send(NULL, 0, MPI_CHAR, requesting_rank, MSG_JOB_STEAL_RESPONSE, pfind_com);
            CHECK_MPI
          }
        }
        debug("[%d] stole %d \n", pfind_rank, work_received);
        pending_work = work_received;
        if (work_received > 0){
          have_processed_work_after_token = 1;
        }
        res->monitor.work_stolen += work_received;

        res->monitor.job_steal_mpitime_us += pfind_timer_diff_usec(& t_start);
      }
    }
  }
  debug("[%d] ended\n", pfind_rank);

  #ifdef LZ4
  free(compress_buf);
  #endif

  free(work);
  MPI_Buffer_detach(bsend_buf, & bsend_size);

  double end = MPI_Wtime();
  res->runtime = end - start;

  if(runtime.logfile != stdout){
    fclose(runtime.logfile);
  }

  res->rate = res->total_files / res->runtime;
  res->com = pfind_com;

  return res;
}

pfind_find_results_t * pfind_aggregrate_results(pfind_find_results_t * local){
  pfind_find_results_t * res = smalloc(sizeof(pfind_find_results_t));
  if(! res){
    pfind_abort("No memory");
  }
  memcpy(res, local, sizeof(*res));

  MPI_Reduce(pfind_rank == 0 ? MPI_IN_PLACE : & res->errors, & res->errors, 5, MPI_LONG_LONG_INT, MPI_SUM, 0, local->com);
  MPI_Reduce(pfind_rank == 0 ? MPI_IN_PLACE : & res->runtime, & res->runtime, 1, MPI_DOUBLE, MPI_MAX, 0, local->com);

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

  res->total_files++;

  if (lstat(dir, & buf) == 0) {
    check_buf(buf, dir);
  } else {
    res->errors++;
    fprintf(runtime.logfile, "Error stating file: %s\n", dir);
  }
}

static int find_do_readdir(char *path, uint64_t dir_start, uint64_t dir_end) {
    char dir[PATH_MAX];
    sprintf(dir, "%s%s", start_dir, path);
    path = & dir[start_dir_length];

    DIR *d;
    if( current_dir.dir ){
      d = current_dir.dir;
    }else{
      d = opendir(dir);
      if (!d) {
          if(opt->verbosity > 1){
            fprintf(runtime.logfile, "Cannot open '%s': %s\n", dir, strerror (errno));
          }
          res->errors++;
          return 0;
      }
      if(opt->parallel_single_dir_access){
        if(dir_start == 0 && dir_end == 0){
          dir_end = LONG_MAX;
        }
        if(dir_start != 0){
          seekdir(d, dir_start);
        }
      }
    }
    int fd = dirfd(d);
    int processed = 0;

    while (1) {
        uint64_t pos = 0;
        if(opt->parallel_single_dir_access){
          pos = telldir(d);
          debug("[%d] processing %llu\n", pfind_rank, (long long unsigned) pos);

          if(pos < dir_start){
            fprintf(runtime.logfile, "Error, telldir() returned smaller value, hashing doesn't work\n");
            exit(1);
          }
          if(pos >= dir_end){
            debug("[%d] reached end %llu >= %llu\n", pfind_rank, (long long unsigned) pos, (long long unsigned) dir_end);
            // done processing with hashing
            closedir(d);
            current_dir.dir = NULL;
            return 0;
          }
        }
        //printf("find_do_readdir %s - %s\n", dir, path);
        if(processed > opt->max_entries_per_iter){
          debug("[%d] interrupting readdir\n", pfind_rank);
          // break criteria to allow continuation of job stealing and such
          if(current_dir.dir == NULL){
            strcpy(current_dir.name, path);
            current_dir.dir = d;
          }
          current_dir.pos_cur = pos;
          current_dir.pos_end = dir_end;
          return 1;
        }
        struct dirent *entry;
        entry = readdir(d);
        if (opt->stonewall_timer && MPI_Wtime() >= runtime.stonewall_endtime ){
          if(opt->verbosity > 1){
            fprintf(runtime.logfile, "Hit stonewall at %.2fs\n", MPI_Wtime());
          }
          break;
        }
        if (entry == NULL) {
            break;
        }
        //debug("[%d] %s\n", pfind_rank, entry->d_name);
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        res->checked_dirents++;
        processed++;

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
          // compare file name
          if(opt->name_pattern && regexec(& opt->name_regex, entry->d_name, 0, NULL, 0) ){
            res->total_files++;
            if(opt->verbosity >= 2){
              printf("Name does not match: %s\n", entry->d_name);
            }
            continue;
          }
          if(! runtime.needs_stat){
            res->total_files++;
            // optimization to skip stat
            res->found_files++;
            if(! opt->just_count){
              fprintf(runtime.logfile, "%s/%s\n", dir, entry->d_name);
            }
            continue;
          }
        }

        // we need to perform a stat operation
        if(enqueue_work(typ, path, entry->d_name)){
          if(current_dir.dir == NULL){
            strcpy(current_dir.name, path);
            current_dir.dir = d;
          }

          if(typ == 'd'){
            static int printed_warning = 0;
            if(! printed_warning){
              printf("[%d] WARNING, utilizing excess queue for processing of the directory %s as the queue is full, supressing further messages. This may lead to suboptimal performance. Try to increase the queue size.\n", pfind_rank, entry->d_name);
              printed_warning = 1;
            }
            enqueue_dir_excess(path, entry->d_name);
          }else{
            char cur_path[PATH_MAX];
            sprintf(cur_path, "%s/%s", path, entry->d_name);
            find_do_lstat(cur_path);
          }
          return 1;
        }
    }
    closedir(d);
    current_dir.dir = NULL;
    return 0;
}
