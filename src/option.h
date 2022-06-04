#ifndef _PFIND_OPTION_H
#define _PFIND_OPTION_H

#include <stdint.h>

/*
 * Initial version by JK
 */

typedef enum{
  OPTION_FLAG,
  OPTION_OPTIONAL_ARGUMENT,
  OPTION_REQUIRED_ARGUMENT
} option_value_type;

typedef struct{
  char shortVar;
  char * longVar;
  char * help;

  option_value_type arg;
  char type;  // data type, H = hidden string
  void * variable;
} option_help;

#define LAST_OPTION {0, 0, 0, (option_value_type) 0, 0, NULL}

int64_t pstring_to_bytes(char *size_str);
void poption_print_current(option_help * args);
int poption_parse(int argc, char ** argv, option_help * options);

#endif
