#include <stdio.h>
#include <getopt.h>
#include <string.h>
#include <stdlib.h>
#include <mpi.h>

#include "pfind-options.h"


// $io500_find_cmd $io500_workdir -newer $timestamp_file -size ${mdt_hard_fsize}c -name *01*

static void pfind_abort(char * str){
  printf(str);
  exit(1);
}

static void pfind_print_help(pfind_options_t * res){
  printf("pfind \nSynopsis:\n"
      "pfind <workdir> [-newer <timestamp file>] [-size <size>c] [-name <substr>]\n"
      "\tworkdir = \"%s\"\n"
      "\t-newer = \"%s\"\n"
      "\t-name = \"%s\"\n"
      "Optional flags\n"
      "\t-C: don't ouput file names just count the number of files found\n"
      "\t-s <seconds>: Stonewall timer for find = %d\n"
      "\t-h: prints the help\n"
      "\t--help: prints the help without initializing MPI\n"
      "\t-r <results_dir>: Where to store results = %s\n"
      "\t-v: increase the verbosity, use multiple times to increase level = %d\n",
      res->workdir,
      res->timestamp_file,
      res->name,
      res->stonewall_timer,
      res->results_dir,
      res->verbosity
    );
}

pfind_options_t * pfind_parse_args(int argc, char ** argv, int force_print_help){
  pfind_options_t * res = malloc(sizeof(pfind_options_t));
  memset(res, 0, sizeof(pfind_options_t));
  int print_help = force_print_help;

  res->workdir = "./";
  res->results_dir = "./pfind-results/";
  res->verbosity = 0;
  res->timestamp_file = NULL;
  res->name = NULL;
  res->size = (uint64_t) -1;
  char * none = "";

  for(int i=0; i < argc - 1; i++){
    if(strcmp(argv[i], "-newer") == 0){
      res->timestamp_file = strdup(argv[i+1]);
      argv[i] = none;
      argv[++i] = none;
    }else if(strcmp(argv[i], "-size") == 0){
      char * str = argv[i+1];
      char extension = str[strlen(str)-1];
      str[strlen(str)-1] = 0;
      res->size = atoll(str);
      switch(extension){
        case 'c': break;
        default:
          pfind_abort("Unsupported exension for -size\n");
      }
      argv[i] = none;
      argv[++i] = none;
    }else if(strcmp(argv[i], "-name") == 0){
      res->name = strdup(argv[i+1]);
      argv[i] = none;
      argv[++i] = none;
    }
  }

  int c;
  while ((c = getopt(argc, argv, "Cs:r:vh")) != -1) {
    if (c == -1) {
        break;
    }

    switch (c) {
    case 'C':
      res->just_count = 1; break;
    case 'h':
      print_help = 1; break;
    case 'r':
      res->results_dir = strdup(optarg); break;
    case 's':
      res->stonewall_timer = atol(optarg);
      break;
    case 'v':
      res->verbosity++; break;
    case 'w':
      res->workdir = strdup(optarg); break;
    case 0:
      break;
    }
  }
  if(print_help){
    pfind_print_help(res);
    int init;
    MPI_Initialized( & init);
    if(init){
      MPI_Finalize();
    }
    exit(0);
  }
  return res;
}
