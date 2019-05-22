// C program to demonstrate use of fork() and pipe()
#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>
#include<sys/types.h>
#include<string.h>
#include<sys/wait.h>

int main(int argc, char const *argv[]) {
  int i,j,numMappers=atoi(argv[1]),numReducers=atoi(argv[2]),status=0;
  int childpid;
  int fd1[2]; // Used to store two ends of first pipe
  if (pipe(fd1)==-1)
	{
		fprintf(stderr, "Pipe Failed" );
		return 1;
	}
  char input_str[]="hola";
  //for mappers
  for (i = 0; i < numMappers; i++) {
    if ((childpid = fork()) < 0) {
      perror("fork:");
      exit(1);
    }
    if (childpid == 0) {
      printf("hijoM %d\n",i);
      break;
    }
  }

  if (childpid > 0) {
    printf("padreM\n");
    //for reducers
    for (j = 0; j < numReducers; j++) {
      if ((childpid = fork()) < 0) {
        perror("fork:");
        exit(1);
      }
      if (childpid == 0) {
        printf("hijoR %d\n",j );
        break;
      }
    }


  }
  if (childpid > 0) {
    printf("padreR\n");
   status=0;
  }
  //for wait mappers
  for (i = 0; i < numMappers; i++) {
    wait(& status);
  }
  //for wait reducers
  for (j = 0; j < numReducers; j++) {
    wait(& status);
  }

  return 0;
}
