#include <stdio.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <assert.h>
#include <string.h>

// This program tests the behavior of the POSIX dir operations to check for potential optimizations
// Compile with: gcc test/pfind-behavior.c -o pfind-behavior
// run with: ./pfind-behavior <DIRECTORY TO TEST>

static char out_names[102][1024];

void u_check_dir_behavior_prepare(char * const dirname){
  int ret;
  ret = mkdir(dirname, S_IRWXU);
  if(ret != 0){
    printf("Error cannot create the directory %s for testing\n", dirname);
  }
  for(int i=0; i < 100; i++){
    sprintf(out_names[i], "%04d-%04d", 100 - i, i);
    char name[1024];

    sprintf(name, "%s/%s", dirname, out_names[i]);
    FILE * fd = fopen(name, "w");
    fclose(fd);
  }
  sprintf(out_names[100], ".");
  sprintf(out_names[101], "..");
}

typedef struct {
  int incrementing;
  int decrementing;
  int hashing_balanced; // are the hashes equally distributed somehow
} dir_seek_behavior;

void u_check_dir_behavior(char * const dirname, dir_seek_behavior * inout_behavior);

int u_update_found_file(char * dirent, int arr[102]){
  for(int i=0; i < 102; i++){
    if(strcmp(out_names[i], dirent) == 0){
      arr[i]++;
      return 1;
    }
  }
  return 0;
}

void u_check_dir_behavior(char * const dirname, dir_seek_behavior * behave){
  memset(behave, 0, sizeof(dir_seek_behavior));

  DIR * dir = opendir(dirname);
  assert(dir != NULL);
  long long unsigned pos;
  long last = 0;

  struct dirent * de;
  int incrementing = 1;
  int decrementing = 0;
  do{
    de = readdir(dir);
    if(de == NULL) break;
    pos = telldir(dir);
    if (pos <= last){
      incrementing = 0;
    }
    if (pos > last){
      decrementing = 0;
    }
    last = pos;
  }while(1);
  closedir(dir);

  behave->decrementing = decrementing;
  behave->incrementing = incrementing;

  int hash_worked = 0;

  printf("%lu\n", sizeof(long));

  // explore binary search space:
  for(int i=0; i < 64; i++){
    unsigned long long loc = 1l<<i;
    dir = opendir(dirname);
    seekdir(dir, loc);
    de = readdir(dir);
    if(de == NULL){
      continue;
    }
    hash_worked++;
    printf("%llu: %s\n", loc, de->d_name);
    closedir(dir);
  }

  //printf("Hashing worked: %d\n", hash_worked);
  int hash_worked_space = 0;
  int fragments = 100;

  int found[102];
  memset(found, 0, sizeof(int)*102);
  for(int i=0; i < fragments; i++){
    dir = opendir(dirname);
    unsigned long loc = 1ul<<63;
    loc = loc / fragments * i;
    seekdir(dir, loc);
    de = readdir(dir);
    if(de == NULL){
      continue;
    }
    printf("Use cookie: %lu found: %s\n", loc, de->d_name);
    if(u_update_found_file(de->d_name, found)){
      hash_worked_space++;
    }
    closedir(dir);
  }

  for(int i=0; i < 102; i++){
    if(found[i] > 10){
      printf("Hashing produced imbalance, found file %s %d times, this is not expected\n", out_names[i], found[i]);
      hash_worked_space = 0;
    }
  }

  if (hash_worked > 50 && hash_worked_space > 40){
    behave->hashing_balanced = 1;
  }
}

int main(int argc, char ** argv){
  char * name;
  if (argc > 1){
    name = argv[1];
  }else{
    name = "testdir";
  }
  printf("Using directory \"%s\" for testing\n", name);
  dir_seek_behavior b;
  u_check_dir_behavior_prepare(name);
  u_check_dir_behavior(name, & b);
  printf("Hashing seems to work: %d\n", b.hashing_balanced);
  printf("Sequential: %d\n", b.incrementing);
  return 0;
}
