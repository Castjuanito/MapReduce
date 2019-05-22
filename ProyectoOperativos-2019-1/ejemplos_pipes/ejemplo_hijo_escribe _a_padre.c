// C program to illustrate
// pipe system call in C
// shared by Parent and Child
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
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
            for (int j = 0; j < 3; j++)
            {
                wait(NULL);
            }
        }

        if (pid == 0)
        {
            //printf("%d\n", getpid());
            char buffer[MSGSIZE];
            memset(&buffer, 0, sizeof(buffer)); // zero out the buffer
            sprintf(buffer, "%d", getpid());
            write(p[1], buffer, MSGSIZE);
            // Adding this line will
            // not hang the program
            close(p[1]);
            //wait(NULL);

            exit(0);
        }
    }
    //close(p[1]);
    while ((nbytes = read(p[0], inbuf, MSGSIZE)) > 0)
    {
        printf("(%s)-\n", inbuf);
        break;
    }
    //close(p[1]);
    while ((nbytes = read(p[0], inbuf, MSGSIZE)) > 0)
    {
        printf("(%s)-\n", inbuf);
        break;
    }
    while ((nbytes = read(p[0], inbuf, MSGSIZE)) > 0)
    {
        printf("(%s)\n", inbuf);
        break;
    }

    printf("Finished reading\n");
    return 0;
}
