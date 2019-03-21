//*****************************************************************

// Realizado por: Juan castañeda, Juan Linares, jonathan Molina

//*****************************************************************

#ifndef MAP_H
#define MAP_H

//*****************************************************************
//Estructura
//*****************************************************************
struct Map
{
  int size;
  int *key;
  int *value;
}Map;

//*****************************************************************
//funciones
//*****************************************************************
struct Map new(struct Map *this, int size);
int size(struct Map *this);
void insert(struct Map *this, int key ,  int value);


#endif
