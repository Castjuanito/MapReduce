//*****************************************************************

// Realizado por: Juan castañeda, Juan Linares, jonathan Molina

//Compilación: "gcc -o analogh analogh.c -lpthread"
//Ejecución: "$ ./analogh -f [logfile] -l [lineas] -m [nmappers] -r [nreducers]"

//*****************************************************************

//*****************************************************************
//Librerias utilizadas
//*****************************************************************
#include "../Model/map.h"
#include "../Model/map.c"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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

//*****************************************************************
//Definicion de funciones
//*****************************************************************
int **rlog(int n, char *logfile);
void busqueda(struct posicion *posiciones_m, struct posicion *posiciones_r);

//*****************************************************************
//Funciones de los hilos
//*****************************************************************
void *map(void *po)
{
  int i;
  struct posicion *f;
  f = (struct posicion *)po;
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

void *reduce(void *po)
{
  int i;
  struct posicion *f;
  f = (struct posicion *)po;
  for (i = f->inicio; i < f->final; i++)
  {
    acumulador[f->indice] += size(&mapas[i]);
  }
}

//*****************************************************************
//Programa Principal
//*****************************************************************
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
}

//*****************************************************************
//Funciones
//*****************************************************************

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

  pthread_t threads_m[NUM_MAPPERS]; // arrglo de identificadores
  pthread_t threads_r[NUM_REDUCERS];

  for (t = 0; t < NUM_MAPPERS; t++)
  {

    rcm = pthread_create(&threads_m[t], NULL, map, (struct posicion *)&posiciones_m[t]);
    if (rcm)
    {
      perror("Hello World");
      exit(-1);
    }
  }
  for (j = 0; j < NUM_MAPPERS; j++)
  {
    pthread_join(threads_m[j], NULL);
  }

  for (t = 0; t < NUM_REDUCERS; t++)
  {

    rcr = pthread_create(&threads_r[t], NULL, reduce, (struct posicion *)&posiciones_r[t]);
    if (rcr)
    {
      perror("Hello World");
      exit(-1);
    }
  }
  for (j = 0; j < NUM_REDUCERS; j++)
  {
    pthread_join(threads_r[j], NULL);
  }
  for (t = 0; t < NUM_REDUCERS; t++)
  {
    k += acumulador[t];
    printf("%d\n", acumulador[t]);
    acumulador[t] = 0;
  }

  printf("%d registros satisfacen los criterios de busqueda\n\n", k);
  gettimeofday(&t1, NULL);
  unsigned int ut1 = t1.tv_sec * 1000000 + t1.tv_usec;
  unsigned int ut0 = t0.tv_sec * 1000000 + t0.tv_usec;
  printf("duracion del programa: %f\n\n", (ut1 - ut0) / 100.0);
}
