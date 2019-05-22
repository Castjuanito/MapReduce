
// C program to illustrate
// pipe system call in C
// shared by Parent and Child
#include <stdio.h>
#include <unistd.h>
#include<stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>

#define MSGSIZE 16
char *msg1 = "hello, world #1";
char *msg2 = "hello, world #2";
char *msg3 = "hello, world #3";

int main()
{
    char inbuf[MSGSIZE];
    int p[2], pid, nbytes;

    if (pipe(p) < 0)
        exit(1);
    for (int i = 0; i < 3; i++)
    {
        /* continued */
        if ((pid = fork()) > 0)
        {
            
            write(p[1], msg1, MSGSIZE);
            write(p[1], msg2, MSGSIZE);
            write(p[1], msg3, MSGSIZE);

            // Adding this line will
            // not hang the program
            close(p[1]);
            //wait(NULL);
        }
        if (pid == 0)
        {
            printf("%d", getpid());
            // Adding this line will
            // not hang the program
            close(p[1]);
            while ((nbytes = read(p[0], inbuf, MSGSIZE)) > 0){
                printf("%s\n", inbuf);
                break;
            }
            if (nbytes != 0)
                exit(2);
            
            exit(0);
        }
        
    }
    for (int j = 0; j < 3; j++)
    {
        wait(NULL);   
    }
    printf("Finished reading\n");
    return 0;
}
