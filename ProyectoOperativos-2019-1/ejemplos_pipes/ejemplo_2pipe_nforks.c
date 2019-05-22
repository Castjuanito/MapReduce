// C program to illustrate
// pipe system call in C
// shared by Parent and Child
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>

#define MSGSIZE 100
char *msg1 = "hello, world #1";
char *msg2 = "hello, world #2";
char *msg3 = "hello, world #3";

int main()
{
    int fd1[2]; // Used to store two ends of first pipe
    int fd2[2]; // Used to store two ends of second pipe
    char fixed_str[] = "forgeeks.org";
    char input_str1[MSGSIZE];
    char input_str2[MSGSIZE];
    char input_str3[MSGSIZE];
    pid_t child_pid, wpid;
    int status = 0;
    int pid, nbytes1, nbytes2;

    if (pipe(fd1) == -1)
    {
        fprintf(stderr, "Pipe Failed");
        return 1;
    }
    if (pipe(fd2) == -1)
    {
        fprintf(stderr, "Pipe Failed");
        return 1;
    }

    scanf("%s", input_str1);
    scanf("%s", input_str2);
    scanf("%s", input_str3);

    // Adding this line will
    // not hang the program

    for (int i = 0; i < 3; i++)
    {
        /* continued */
        if ((pid = fork()) > 0)
        {
            write(fd1[1], input_str1, MSGSIZE);
            write(fd1[1], input_str2, MSGSIZE);
            write(fd1[1], input_str3, MSGSIZE);
            close(fd1[1]);
            //close(fd1[0]); // Close reading end of first pipe

            //while ((wpid = wait(&status)) > 0)    ;
           
        }

        if (pid == 0)
        {
            printf("%d\n", getpid());
            // Adding this line will
            // not hang the program
            close(fd1[1]);
            close(fd2[0]);
            char concat_str[100];

            while ((nbytes2 = read(fd1[0], concat_str, MSGSIZE)) > 0)
            {
                //    printf("\n%s\n", concat_str);
                int k = strlen(concat_str);
                int i;
                for (i = 0; i < strlen(fixed_str); i++)
                    concat_str[k++] = fixed_str[i];

                concat_str[k] = '\0';
                printf("--%s\n", concat_str);
                

                // Write concatenated string and close writing end
                write(fd2[1], concat_str, MSGSIZE);
                close(fd2[1]);
                break;
            }

            //close(fd2[0]);

            exit(0);
        }
    }
     for (int j = 0; j < 3; j++)
            {
                wait(NULL);
            }
    close(fd2[1]); // Close writing end of second pipe
    char concat_str1[100];
    while ((nbytes1 = read(fd2[0], concat_str1, MSGSIZE)) > 0)
    {
        printf("Concatenated string %s\n", concat_str1);
        break;
    }

    while ((nbytes1 = read(fd2[0], concat_str1, MSGSIZE)) > 0)
    {
        printf("Concatenated string %s\n", concat_str1);
        break;
    }

    while ((nbytes1 = read(fd2[0], concat_str1, MSGSIZE)) > 0)
    {
        printf("Concatenated string %s\n", concat_str1);
        break;
    }




    //close(fd2[0]);

    printf("Finished reading\n");

    // Read string from child, print it and close
    // reading end.

    return 0;
}
