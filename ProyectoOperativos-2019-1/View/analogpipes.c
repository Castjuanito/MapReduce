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

int main(int argc, char const *argv[])
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
            pr.final = NUM_MAPPERS;
            pr.indice = t;
        }
        else
        {
            pr.inicio = aux;
            pr.final = aux + rango_r;
            pr.indice = t;
        }
        aux += rango_r;
        posiciones_r[t] = pr;
    }

    printf("\n");
    _Bool salir = FALSE;
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
            busqueda(posiciones_m, posiciones_r);
        }
        else
            exit(0);
    } while (salir == FALSE);
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
    const char s[2] = " ";
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

//*********************************************************************************************************
//Funciones
//Realiza las busquedas que ingresa un susario mediante la invocacion de hilos para los mappers y reducers
//Recibe unos arreglos con las pociciones y rangos que deben cubrir los hilos dentro de los registros
//*********************************************************************************************************
void busqueda(struct posicion *posiciones_m, struct posicion *posiciones_r)
{
    char parametros[50];
    signo = (char *)malloc(5 * sizeof(char));
    int t, j, k = 0, rcr, rcm;
    clock_t ti;
    const char s[] = " ,";
    char *token, val[3][50];
    struct timeval t0, t1;
    gettimeofday(&t0, NULL);

    printf("Ingrese parametros: Columna, Signo, valor \n");
    printf("$ ");

    getchar();
    fgets(parametros, 50, stdin);
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
        return;
    }

    columna = atoi(val[0]);

    strcpy(signo, val[1]);

    valor = atoi(val[2]);

    ti = clock();
    int i, j,status = 0;
    int childpid;
    int fd1[2]; // Used to store two ends of first pipe
    if (pipe(fd1) == -1)
    {
        fprintf(stderr, "Pipe Failed");
        return 1;
    }
    char input_str[] = "hola";
    //for mappers
    for (i = 0; i < NUM_MAPPERS; i++)
    {
        if ((childpid = fork()) < 0)
        {
            perror("fork:");
            exit(1);
        }
        if (childpid == 0)
        {
            printf("hijoM %d\n", i);
            break;
        }
    }

    if (childpid > 0)
    {
        printf("padreM\n");
        //for reducers
        for (j = 0; j < NUM_REDUCERS; j++)
        {
            if ((childpid = fork()) < 0)
            {
                perror("fork:");
                exit(1);
            }
            if (childpid == 0)
            {
                printf("hijoR %d\n", j);
                break;
            }
        }
    }
    if (childpid > 0)
    {
        printf("padreR\n");
        status = 0;
    }
    //for wait mappers
    for (i = 0; i < NUM_MAPPERS; i++)
    {
        wait(&status);
    }
    //for wait reducers
    for (j = 0; j < NUM_REDUCERS; j++)
    {
        wait(&status);
    }
}
