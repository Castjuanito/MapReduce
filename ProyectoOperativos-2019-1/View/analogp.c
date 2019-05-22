/**************************
 -----Realizado por: Jonathan Molina, Juan Pablo Linarez,
 -----Compilación: "gcc analogp.c -o anologp"
 -----Ejecución: "./analogp -f logfile -l lineas -m mmappers -r nreducers -i intermedios"
*/
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>
#include <wait.h>
#include <string.h>
#include <stdbool.h>
#include <time.h>
#include <sys/time.h>

/***************************
Variables globales
*/
#define MAXCHAR 10000
/***************************
Funciones
*/
int menu();
void generarArchivos(char *nomArchivo, int cantidad, int numMappers, int sobrante);
void fmapper(int arch, char *argumento, char *comparacion, char *limite, int tam, int inter);
int fReduce(int pos, int cantidad, int sobrante, bool si, int inter);
/***************************
Programa main
*/
int main(int argc, char **argv)
{
  if (argc != 6)
  {
    printf("Argumentos invalidos, los argumentos que se necesitan son: logfile, 			lineas,mmappers,nreducers,intermedios\n");
    return EXIT_FAILURE;
  }
  else
  {
    // variables locales
    char *nomArchivo = argv[1];
    char consulta[50];
    char *argumento = (char *)malloc(5);
    char *comparacion = (char *)malloc(5);
    char *limite = (char *)malloc(5);
    char *token;
    char *copia;
    bool si = false;
    int numLineas = atoi(argv[2]);
    int numMappers = atoi(argv[3]);
    int numReduce = atoi(argv[4]);
    int interm = atoi(argv[5]);
    int opcion = 0, i = 0, j = 0, status = 0;
    int childpid;
    int tam;
    int R = 0;
    int contArch = 0;
    int resultado = 0;
    int cantidadLineas = numLineas / numMappers;
    int sobrante = numLineas % numMappers;
    int archreduce = numMappers / numReduce;
    int archsob = numMappers % numReduce;
    int auxsob = archsob;
    int bandera = 0;
    struct timeval t0, t1;
    gettimeofday(&t0, NULL);
    generarArchivos(nomArchivo, cantidadLineas, numMappers, sobrante);
    do
    {
      //menu
      opcion = menu();
      bandera = 0;
      if (opcion == 1)
      {
        printf("Digite la consula que desea realizar: columna, signo, valor\n");
        getchar();
        fgets(consulta, 50, stdin);

        copia = consulta;
        token = strtok(copia, ", ");
        argumento = token;
        token = strtok(NULL, ", ");
        comparacion = token;
        token = strtok(NULL, ", ");
        limite = token;
        if (argumento == NULL || comparacion == NULL || limite == NULL)
        {
          printf("error en el formato de la consulta");
          bandera = 1;
        }
        else
        {
          for (i = 0; i < numMappers; i++)
          {

            if ((childpid = fork()) < 0)
            {
              perror("fork:");
              exit(1);
            }
            else
            {
              if (sobrante > 0)
              {
                tam = cantidadLineas + 1;
                sobrante--;
              }
              else
              {
                tam = cantidadLineas;
              }
            }
            if (childpid == 0)
            {
              fmapper(i + 1, argumento, comparacion, limite, tam, interm);
              break;
            }
          }
          for (j = 0; j < numMappers; ++j)
          {
            wait(&status);
          }
          if (childpid > 0)
          {
            for (i = 0; i < numReduce; i++)
            {
              if ((childpid = fork()) < 0)
              {
                perror("fork:");
                exit(1);
              }
              else
              {
              }
              if (childpid == 0)
              {
                contArch = archreduce;
                if (i == (numReduce - 1))
                {
                  si = true;
                }
                resultado = fReduce(i, contArch, auxsob, si, interm);
                return (resultado);
              }
            }
            for (j = 0; j < numReduce; j++)
            {
              wait(&status);
              if (WIFEXITED(status))
                R = R + WEXITSTATUS(status);
            }
          }
        }
        if (childpid > 0)
        {
          printf("\n\nEl resultado es %d\n", R);
          R = 0;
          status = 0;
          gettimeofday(&t1, NULL);
          unsigned int ut1 = t1.tv_sec * 1000000 + t1.tv_usec;
          unsigned int ut0 = t0.tv_sec * 1000000 + t0.tv_usec;
          printf("duracion del programa: %f\n", (ut1 - ut0) / 100.0);
        }
      }
      else
      {
        printf("Hasta luego\n");
        exit(1);
      }
    } while (opcion != 2 && (childpid > 0 || bandera == 1));
  }
  return 0;
}
int menu()
{
  int opcion = 0;
  printf("\n1. Realizar una consulta\n");
  printf("2. Salir del Sistema\n");
  scanf("%d", &opcion);
  /*if(opcion>2 || opcion<1){
      printf("Opcion invalida, vuelva a intentarlo\n");
      return menu();
   }*/
  return opcion;
}
void generarArchivos(char *nomArchivo, int cantidad, int numMappers, int sobrante)
{
  FILE *fp;
  FILE *fw;
  char *buffer = (char *)malloc(MAXCHAR);
  char *nomindividual = (char *)malloc(2);
  int contador = 0, i;
  int numLineas = cantidad;
  int auxs = sobrante;
  fp = fopen(nomArchivo, "r");
  if (fp == NULL)
  {
    perror("Error al leer el archivo fuente");
    exit(-1);
  }
  else
  {
    for (contador = 0; contador < numMappers; contador++)
    {
      sprintf(nomindividual, "%d", contador);
      fw = fopen(nomindividual, "w");
      if (auxs > 0)
      {
        auxs--;
        numLineas = numLineas + 1;
      }
      for (i = 0; i < numLineas; i++)
      {
        fgets(buffer, MAXCHAR, fp);
        fputs(buffer, fw);
      }
      numLineas = cantidad;
      fclose(fw);
    }
  }
  fclose(fp);
  free(buffer);
  free(nomindividual);
}
void fmapper(int arch, char *argumento, char *comparacion, char *limite, int tam, int inter)
{
  char *buffer = (char *)malloc(4);
  char *val = (char *)malloc(10);
  char *lectura = (char *)malloc(MAXCHAR);
  char *llave = (char *)malloc(2);
  char *token;
  char *aux;
  int valor = 0;
  int arg = atoi(argumento);
  int lim = atoi(limite);
  int i = 0, j = 0;
  sprintf(buffer, "%d", arch - 1);
  FILE *fp;
  FILE *fw;
  fp = fopen(buffer, "r");
  if (fp == NULL)
  {
    perror("Error al leer el archivo fuente---1");
    exit(-1);
  }
  else
  {
    sprintf(lectura, "%d", arch * 100);
    fw = fopen(lectura, "w");
    if (fw == NULL)
    {
      perror("Error al leer el archivo fuente---2");
      exit(-1);
    }
    for (i = 0; i < tam; i++)
    {
      fgets(lectura, MAXCHAR, fp);
      aux = lectura;
      token = NULL;
      token = strtok(aux, " ");
      llave = token;
      for (j = 0; j < arg - 1; j++)
      {
        token = strtok(NULL, " ");
      }
      val = token;
      valor = atoi(val);
      if (*comparacion == '<')
      {
        if (valor < lim)
        {
          fputs(llave, fw);
          fputs(" ", fw);
          sprintf(llave, "%d", valor);
          fputs(llave, fw);
          fputs("\n", fw);
        }
      }
      else
      {
        if (*comparacion == '>')
        {
          if (valor > lim)
          {
            fputs(llave, fw);
            fputs(" ", fw);
            sprintf(llave, "%d", valor);
            fputs(llave, fw);
            fputs("\n", fw);
          }
        }
        else
        {
          if (strcmp(comparacion, "<=") == 0)
          {
            if (valor <= lim)
            {
              fputs(llave, fw);
              fputs(" ", fw);
              sprintf(llave, "%d", valor);
              fputs(llave, fw);
              fputs("\n", fw);
            }
          }
          else
          {
            if (strcmp(comparacion, ">=") == 0)
            {
              if (valor >= lim)
              {
                fputs(llave, fw);
                fputs(" ", fw);
                sprintf(llave, "%d", valor);
                fputs(llave, fw);
                fputs("\n", fw);
              }
            }
            else
            {
              if (*comparacion == '=')
              {
                if (valor == lim)
                {
                  fputs(llave, fw);
                  fputs(" ", fw);
                  sprintf(llave, "%d", valor);
                  fputs(llave, fw);
                  fputs("\n", fw);
                }
              }
              else
              {
                printf("\nerror en el operador de comparacion dado, argumento no valido\n");
              }
            }
          }
        }
      }
    }
    fclose(fw);
  }
  fclose(fp);
  if (inter == 0)
  {
    if (remove(buffer) != 0)
    {
      printf("El archivo no se pudo eliminar");
    }
  }
}
int fReduce(int pos, int cantidad, int sobrante, bool si, int inter)
{
  FILE *fp;
  char *buffer = (char *)malloc(4);
  char *linea = (char *)malloc(MAXCHAR);
  int auxcant = cantidad;
  int posicion = pos * cantidad;
  int cont = 0;
  if (si == true)
  {
    auxcant = auxcant + sobrante;
  }
  while (auxcant != 0)
  {
    sprintf(buffer, "%d", (posicion + 1) * 100);
    fp = fopen(buffer, "r");
    if (fp == NULL)
    {
      perror("Error al leer el archivo fuente---3");
      exit(-1);
    }
    else
    {
      fgets(linea, MAXCHAR, fp);
      while (!feof(fp))
      {
        fgets(linea, MAXCHAR, fp);
        cont++;
      }
    }
    fclose(fp);
    if (inter == 0)
    {
      if (remove(buffer) != 0)
      {
        printf("El archivo no se pudo eliminar");
      }
    }

    posicion++;
    auxcant--;
  }
  return cont;
  free(buffer);
}
