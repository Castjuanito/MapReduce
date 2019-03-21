#include "map.h"
#include "map.c"
#include <stdio.h>
#include <stdlib.h>

int main(int argc, char* argv[]) {
  int s = 0;
  struct Map m = new(&m);
  printf("salio constructor\n");
  s = size(&m);
  printf("-----%d\n",s );

  insert(&m,1,2);
  s = size(&m);
  printf("-----%d\n",s );
}
