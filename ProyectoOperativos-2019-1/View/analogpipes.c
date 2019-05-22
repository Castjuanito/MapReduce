#include "../Model/map.h"
#include "../Model/map.c"
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>
#include <wait.h>
#include <string.h>
#include <stdbool.h>
#include <time.h>
#include <sys/time.h>

//*****************************************************************
//definiciones de tipo
//*****************************************************************
#ifndef FALSE
#define FALSE (0)
#endif

#ifndef TRUE
#define TRUE (!FALSE)
#endif

typedef struct Map *MapPointer;
//*****************************************************************
//Estructuras
//*****************************************************************
struct posicion
{
    int inicio;
    int final;
    int indice;
};

//*****************************************************************
//Variables globales
//*****************************************************************
struct Map *mapas;
int **matriz;
int lineas;
int *acumulador;
char *nArchivo;
int NUM_MAPPERS;
int NUM_REDUCERS;
char *signo;
int columna, valor;
int child_state;

//*****************************************************************
//Definicion de funciones
//*****************************************************************
int **rlog(int n, char *logfile);
void busqueda(struct posicion *posiciones_m, struct posicion *posiciones_r);
int **createArrOfPipes(int amount);
void Mapper(struct posicion *f);

int main(int argc, char *argv[])
{
    if (argc > 5 || argc < 5)
    {
        printf("parametros incorrectos: cantidad incorrecta\n");
        exit(1);
    }
    int i, j, k = 0, t, rc, aux = 0, rango = 0, cantidad;

    int rango_m = 0, rango_r = 0;
    char *logfile;
    char opcion;
    NUM_MAPPERS = atoi(argv[3]);
    NUM_REDUCERS = atoi(argv[4]);

    if (NUM_MAPPERS < NUM_REDUCERS)
    {
        printf("parametros incorrectos: nmappers debe ser mayor o igual a nreducers\n");
        exit(1);
    }

    if (NUM_MAPPERS < 1 || NUM_REDUCERS < 1)
    {
        printf("parametros incorrectos: nmappers y nreducers deben ser mayores a 0\n");
        exit(1);
    }

    logfile = argv[1];
    lineas = atoi(argv[2]);
    struct posicion posiciones_m[NUM_MAPPERS], pm;
    struct posicion posiciones_r[NUM_REDUCERS], pr;

    float ran_m = 0;
    float ran_r = 0;

    acumulador = (int *)malloc(NUM_REDUCERS * sizeof(int));
    matriz = (int **)malloc(lineas * sizeof(int *));
    for (i = 0; i < lineas; i++)
        matriz[i] = (int *)malloc(18 * sizeof(int));

    mapas = (MapPointer)malloc(NUM_MAPPERS * sizeof(Map));

    matriz = (int **)rlog(lineas, logfile);
    /*for (int as = 0; as < lineas; as++)
    {
        for (int co = 0; co < 18; co++)
        {
            printf("%d ", matriz[as][co]);
        }
        printf("\n");
    }*/

    for (t = 0; t < NUM_MAPPERS; t++)
    {
        ran_m = lineas / NUM_MAPPERS;
        rango_m = (int)(ran_m < 0 ? (ran_m - 0.5) : (ran_m + 0.5));
        if (t == (NUM_MAPPERS - 1))
        {
            pm.inicio = aux;
            pm.final = lineas;
            pm.indice = t;
        }
        else
        {
            pm.inicio = aux;
            pm.final = aux + rango_m;
            pm.indice = t;
        }
        aux += rango_m;
        posiciones_m[t] = pm;
    }
    aux = 0;
    for (t = 0; t < NUM_REDUCERS; t++)
    {
        ran_r = NUM_MAPPERS / NUM_REDUCERS;
        rango_r = (int)(ran_r < 0 ? (ran_r - 0.5) : (ran_r + 0.5));
        if (t == (NUM_REDUCERS - 1))
        {
            pr.inicio = aux;
            pr.final = NUM_MAPPERS-1;
            pr.indice = t;
        }
        else
        {
            pr.inicio = aux;
            pr.final = aux + rango_r - 1;
            pr.indice = t;
        }
        aux += rango_r;
        posiciones_r[t] = pr;
    }

    printf("\n");
    char parametros[50];
    signo = (char *)malloc(5 * sizeof(char));
    //int t, k = 0, rcr, rcm;
    clock_t ti;
    char s[] = " ,";
    char *token, val[3][50];
    struct timeval t0, t1;
    gettimeofday(&t0, NULL);
    int childpid;
    _Bool salir = TRUE;
    pid_t pid[NUM_MAPPERS];
    pid_t pid_reducers[NUM_REDUCERS];
    do
    {
        printf("1.Realizar una consulta \n");
        printf("2.Salir del Sistema \n");

        scanf("%c", &opcion);
        if (opcion == '1')
        {
            for (t = 0; t < NUM_MAPPERS; t++)
            {
                new (&mapas[t], lineas);
            }
            printf("Ingrese parametros: Columna, Signo, valor \n");
            printf("$ ");

            getchar();
            fgets(parametros, 50, stdin);

            ti = clock();
            int i, j, status = 0;
            int nbytes1, nbytes2;
            int fd1[2]; // Used to store two ends of first pipe
            int fd2[2]; // Used to store two ends of first pipe
            int **pipes = createArrOfPipes(NUM_REDUCERS);

            for (int i = 0; i < NUM_REDUCERS; i++)
            {
                if (pipe(pipes[i]))
                {
                    fprintf(stderr, "Pipe Failed");
                    return 1;
                }
            }

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

            //for mappers
            for (i = 0; i < NUM_MAPPERS; i++)
            {
                if ((pid[i] = fork()) < 0)
                {
                    perror("fork:");
                    exit(1);
                }
                if (pid[i] == 0)
                {
                    //printf("hijoM %d\n", i);
                    close(fd1[1]);
                    close(fd2[0]);
                    char concat_str[50];

                    while ((nbytes2 = read(fd1[0], concat_str, 50)) > 0)
                    {
                        //  printf("\n-----%s", concat_str);
                        token = strtok(parametros, s);

                        int a = 0;

                        while (token != NULL)
                        {
                            strcpy(val[a], token);
                            a++;
                            token = strtok(NULL, s);
                        }

                        if (a > 3 || a < 3)
                        {
                            printf("consulta erronea\n");
                            return 1;
                        }

                        columna = atoi(val[0]);

                        strcpy(signo, val[1]);

                        valor = atoi(val[2]);

                        Mapper(&posiciones_m[i]);

                        for (int h = 0; h < NUM_REDUCERS; h++)
                        {
                            //close(pipes[(&posiciones_r[h])->indice][0]);
                            if ((i <= (&posiciones_r[h])->final && i >= (&posiciones_r[h])->inicio))
                            {

                                char buffer[10];
                             //   printf("%d----%d---%d||%d--%d\n", size(&mapas[i]), h, i, (&posiciones_r[h])->inicio, (&posiciones_r[h])->final);
                                memset(&buffer, 0, sizeof(buffer)); // zero out the buffer
                                sprintf(buffer, "%d", size(&mapas[i]));
                                write(pipes[(&posiciones_r[h])->indice][1], &mapas[i], sizeof(struct Map));
                            }
                            close(pipes[(&posiciones_r[h])->indice][1]);
                        }

                        // Write concatenated string and close writing end
                        //write(fd2[1], concat_str, MSGSIZE);
                        // close(fd2[1]);
                        break;
                    }

                    //close(fd2[0]);
                    
                    exit(0);
                }
            }
            //printf("padreM \n");
            for (int m = 0; m < NUM_MAPPERS; m++)
            {
                // printf("\n-----%s", parametros);
                write(fd1[1], parametros, 50);
            }

            close(fd1[1]);

            for (i = 0; i < NUM_MAPPERS; i++)
            {
                pid_t cpid = waitpid(pid[i], &child_state, 0);
                if (WIFEXITED(child_state))
                {
                   // printf("Mapper Child %d terminated with status: %d\n", cpid, WEXITSTATUS(child_state));
                }
            }

            //for reducers

            int nbytes;
            for (j = 0; j < NUM_REDUCERS; j++)
            {
                int child = pid_reducers[j] = fork() ; 
                if (child < 0)
                {
                    perror("fork:");
                    exit(1);
                }
                if (pid_reducers[j] == 0)
                {
                    char concat_str2[10];
                   
                    int ck = 0;
                    struct Map mapa;
                    for (int b = (&posiciones_r[j])->inicio; b <= (&posiciones_r[j])->final; b++)
                    {
                        // close(pipes[j][1]);
                        while ((nbytes1 = read(pipes[j][0], &mapa, sizeof(struct Map)) > 0))
                        {
                          //  printf("** Write Concatenated string %s-----%d\n", concat_str2, j);
                        //    close(fd2[0]);
                        char buffer[10];
                             //   printf("%d----%d---%d||%d--%d\n", size(&mapas[i]), h, i, (&posiciones_r[h])->inicio, (&posiciones_r[h])->final);
                                memset(&buffer, 0, sizeof(buffer)); // zero out the buffer
                                sprintf(buffer, "%d", size(&mapa));
                              //  printf(buffer);
                            write(fd2[1],buffer, 10);
                            
                            break;
                        }
                        
                    }
                    
                    close(pipes[j][0]);
                    close(fd2[1]);
                  //  printf("bye\n");
                    exit(0);
                }
            }
                    close(fd2[1]); 
                    int res=0;
            for (j = 0; j < NUM_REDUCERS; j++)
            {

                pid_t cpid = waitpid(pid_reducers[j], &child_state, 0);

                  //  printf("Reducer Child %d terminated with status: %d\n", cpid, WEXITSTATUS(child_state));
                    char final[10];
            
                   // close(fd2[1]); 
                    while ((nbytes1 = read(fd2[0], final, 10)) > 0)
                    {
                        //printf("read Concatenated string %s\n", final);
                        res += atoi(final);
                    }
                    
                   //  kill(pid_reducers[NUM_REDUCERS], 9);
            }
            

            printf("el resultado es: %d\n",res);
        }
        else
        {
            exit(0);
        }
    } while (salir == TRUE);
    printf("\n");
    return 0;
}
//******************************************************************************************************
//rlog
//Genera un arreglo bidimenconal con base a un archivo que contiene cierta cantidad de registros
//Recibe la cantidad de registros o lineas y una cadena de caracteres con el nombre del archivo a abrir
//Retorna un arreglo bidimencional con el contenido del archivo
//******************************************************************************************************
int **rlog(int n, char *logfile)
{
    int c, x = 0, y = 0, i, j;
    int **mat = (int **)malloc(n * sizeof(int *));
    for (i = 0; i < n; i++)
        mat[i] = (int *)malloc(18 * sizeof(int));
    char *line = NULL;
    size_t len = 0;
    ssize_t read;
    char *token;
    char s[2] = " ";
    FILE *file;
    file = fopen(logfile, "r");
    if (file)
    {
        while ((read = getline(&line, &len, file)) != -1)
        {
            token = strtok(line, s);
            while (token != NULL)
            {
                mat[x][y] = atoi(token);
                y++;
                token = strtok(NULL, s);
            }
            y = 0;
            x++;
        }
        fclose(file);
    }
    return mat;
}

int **createArrOfPipes(int amount)
{
    int **arr, i;
    arr = (int **)malloc(amount * sizeof(int *));
    for (i = 0; i < lineas; i++)
    {
        arr[i] = (int *)malloc(2 * sizeof(int));
    }
    return arr;
}

void Mapper(struct posicion *f)
{
    int i;
    for (i = f->inicio; i < f->final; i++)
    {
        if (strcmp(signo, "<") == 0)
        {
            if (matriz[i][columna - 1] < valor)
            {
                insert(&mapas[f->indice], matriz[i][0], matriz[i][columna - 1]);
            }
        }
        else if (strcmp(signo, ">") == 0)
        {
            if (matriz[i][columna - 1] > valor)
            {
                insert(&mapas[f->indice], matriz[i][0], matriz[i][columna - 1]);
            }
        }
        else if (strcmp(signo, "=") == 0)
        {
            if (matriz[i][columna - 1] == valor)
            {
                insert(&mapas[f->indice], matriz[i][0], matriz[i][columna - 1]);
            }
        }
        else if (strcmp(signo, ">=") == 0)
        {
            if (matriz[i][columna - 1] >= valor)
            {
                insert(&mapas[f->indice], matriz[i][0], matriz[i][columna - 1]);
            }
        }
        else if (strcmp(signo, "<=") == 0)
        {
            if (matriz[i][columna - 1] <= valor)
            {
                insert(&mapas[f->indice], matriz[i][0], matriz[i][columna - 1]);
            }
        }
    }
}