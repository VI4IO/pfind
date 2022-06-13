#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <mpi.h>
#include <regex.h>

#include "pfind-options.h"
#include "option.h"


// $io500_find_cmd $io500_workdir -newer $timestamp_file -size ${mdt_hard_fsize}c -name *01*

void pfind_abort(char * str){
  printf("%s", str);
  exit(1);
}

static void pfind_print_help(pfind_options_t * res, option_help * args){
  printf("pfind \nSynopsis:\n"
      "pfind <workdir> [-newer <timestamp file>] [-size <size>c] [-name <substr>] [-regex <regex>]\n"
      "\tworkdir = \"%s\"\n"
      "\t-newer = \"%s\"\n"
      "\t-name|-regex = \"%s\"\n", res->workdir, res->timestamp_file, res->name_pattern);
  poption_print_help(args);
}

pfind_options_t * pfind_parse_args(int argc, char ** argv, int force_print_help, MPI_Comm com){
  pfind_options_t * res = malloc(sizeof(pfind_options_t));
  memset(res, 0, sizeof(pfind_options_t));
  int print_help = force_print_help;

  res->workdir = "./";
  res->results_dir = NULL;
  res->verbosity = 0;
  res->timestamp_file = NULL;
  res->name_pattern = NULL;
  res->size = UINT64_MAX;
  res->queue_length = 100000;
  res->max_entries_per_iter = 1000;
  char * firstarg = NULL;

  // when we find special args, we process them
  // but we need to replace them with 0 so that getopt will ignore them
  // and getopt will continue to process beyond them
  for(int i=1; i < argc - 1; i++){
    if(strcmp(argv[i], "-newer") == 0){
      res->timestamp_file = strdup(argv[i+1]);
      argv[i][0] = 0;
      argv[++i][0] = 0;
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
      argv[i][0] = 0;
      argv[++i][0] = 0;
    }else if(strcmp(argv[i], "-name") == 0){
      res->name_pattern = malloc(strlen(argv[i+1])*4+100);
      // transform a traditional name pattern to a regex:
      char * str = argv[i+1];
      char * out = res->name_pattern;
      int pos = 0;
      for(unsigned i=0; i < strlen(str); i++){
        if(str[i] == '*'){
          pos += sprintf(out + pos, ".*");
        }else if(str[i] == '.'){
          pos += sprintf(out + pos, "[.]");
        }else if(str[i] == '"' || str[i] == '\"'){
          // erase the "
        }else{
          out[pos] = str[i];
          pos++;
        }
      }
      out[pos] = 0;

      int ret = regcomp(& res->name_regex, res->name_pattern, 0);
      if(ret){
        pfind_abort("Invalid regex for name given\n");
      }
      argv[i][0] = 0;
      argv[++i][0] = 0;
    }else if(strcmp(argv[i], "-regex") == 0){
      res->name_pattern = strdup(argv[i+1]);
      int ret = regcomp(& res->name_regex, res->name_pattern, 0);
      if(ret){
        pfind_abort("Invalid regex for name given\n");
      }
      argv[i][0] = 0;
      argv[++i][0] = 0;
    }else if(! firstarg){
      firstarg = strdup(argv[i]);
      argv[i][0] = 0;
    }
  }
  if(argc == 2){
    firstarg = strdup(argv[1]);
  }

  char * type = NULL;
  option_help args[] = {
    {'E', NULL, "Erase empty directories", OPTION_FLAG, 'd', & res->delete_dirs},
    {'e', NULL, "Erase files matched", OPTION_FLAG, 'd', & res->delete_files},
    {'N', NULL, "steal with 50%% likelihood from the next process instead of randomly", OPTION_FLAG, 'd', & res->steal_from_next},
    {'P', NULL, "output per process for debugging and checks loadbalance", OPTION_FLAG, 'd', & res->print_by_process},
    {'C', NULL, "don't output file names just count the number of files found", OPTION_FLAG, 'd', & res->just_count},
    {'v', NULL, "increase the verbosity, use multiple times to increase level", OPTION_FLAG, 'd', & res->verbosity},
    {'M', NULL, "maximum number of elements to process per readdir iteration", OPTION_OPTIONAL_ARGUMENT, 'd', & res->max_entries_per_iter},
    {'s', NULL, "Stonewall timer for find", OPTION_OPTIONAL_ARGUMENT, 'd', & res->stonewall_timer},
    {'q', NULL, "queue length (max pending ops)", OPTION_OPTIONAL_ARGUMENT, 'd', & res->queue_length},
    {'D', NULL, "[rates]: print rates", OPTION_OPTIONAL_ARGUMENT, 's', & type},
    {'H', NULL, "parallelize single directory access [option 1=hashing, option 2=sequential]", OPTION_OPTIONAL_ARGUMENT, 'd', & res->parallel_single_dir_access},
    {'r', NULL, "Where to store results", OPTION_OPTIONAL_ARGUMENT, 's', & res->results_dir},
    LAST_OPTION
  };  
  
  poption_parse(argc - 1, argv + 1, args);

  if(res->queue_length < 10){
    pfind_abort("Queue must be at least 10 elements!\n");
  }
  if(type){
    if(strcmp(type, "rates") == 0){
      res->print_rates = 1;
    }else{
      pfind_abort("Unsupported debug flag\n");
    }
  }

  if(print_help){
    pfind_print_help(res, args);
    exit(0);
  }
  MPI_Comm_rank(com, & pfind_rank);
  MPI_Comm_size(com, & pfind_size);
  pfind_com = com;
      
  if(res->verbosity > 2 && pfind_rank == 0){
    printf("Regex: %s\n", res->name_pattern);
  }

  if(! firstarg){
    pfind_abort("Error: pfind <directory>\n");
  }
  res->workdir = firstarg;

  return res;
}
