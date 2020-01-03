#include <stdio.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <assert.h>
#include <string.h>

// This program tests the behavior of the POSIX dir operations to check for potential optimizations
// Compile with: gcc test/pfind-behavior.c -o pfind-behavior
// run with: ./pfind-behavior <DIRECTORY TO TEST>

void u_check_dir_behavior_prepare(char * const dirname){
  int ret;
  ret = mkdir(dirname, S_IRWXU);
  if(ret != 0){
    printf("Error cannot create the directory %s for testing\n", dirname);
    return;
  }
  for(int i=0; i < 100; i++){
    char name[1024];
    sprintf(name, "%s/%04d-%04d", dirname, 100 - i, i);
    FILE * fd = fopen(name, "w");
    fclose(fd);
  }
}

typedef struct {
  int incrementing;
  int decrementing;
  int hashing_balanced; // are the hashes equally distributed somehow
} dir_seek_behavior;

void u_check_dir_behavior(char * const dirname, dir_seek_behavior * inout_behavior);

void u_check_dir_behavior(char * const dirname, dir_seek_behavior * behave){
  memset(behave, 0, sizeof(dir_seek_behavior));

  DIR * dir = opendir(dirname);
  assert(dir != NULL);
  long pos;
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

  dir = opendir(dirname);
  int hash_worked = 0;
  // explore binary search space:
  for(int i=0; i < 64; i++){
    unsigned long loc = 1l<<i;
    seekdir(dir, loc);
    de = readdir(dir);
    if(de == NULL){
      continue;
    }
    hash_worked++;
    //printf("%lu: %s\n", loc, de->d_name);
  }
  //printf("Hashing worked: %d\n", hash_worked);
  int hash_worked_space = 0;
  int fragments = 100;
  for(int i=0; i < fragments; i++){
    unsigned long loc = -1;
    loc = loc / fragments * i;
    seekdir(dir, loc);
    de = readdir(dir);
    if(de == NULL){
      continue;
    }
    //printf("%lu: %s\n", loc, de->d_name);
    hash_worked_space++;
  }
  closedir(dir);

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
  printf("Hashing balanced: %d\n", b.hashing_balanced);
  return 0;
}
