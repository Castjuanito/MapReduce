//*****************************************************************

// Realizado por: Juan castañeda, Juan Linares, jonathan Molina

//*****************************************************************

//*****************************************************************
//Librerias utilizadas
//*****************************************************************
#include "map.h"
#include<stdlib.h>
#include <stdio.h>


//*****************************************************************
//Funciones
//*****************************************************************


//*****************************************************************
//size
//Funccion que retorna el tamaño de un mapa dado
//*****************************************************************
int size(struct Map *this) {
	//printf("entro size -- %d \n", this->size );
			return this->size;
}


//*****************************************************************
//Insert
//Funcion que inserrta un nuevo key y value a un mapa dado
//*****************************************************************
void insert(struct Map *this,  int key ,  int value) {
  int nsize = size(this)+1;
	//printf("entro insert----%d\n",nsize);


  this->key[nsize] = key;
  this->value[nsize] = value;
	this->size = this->size+ 1;
	//printf("key-> %d\n", this->key[nsize]);
	//printf("value-> %d\n", this->value[nsize]);
}


//*****************************************************************
//Funciones
//*****************************************************************
struct Map  new(struct Map *this,int size) {
			//printf("entro constructor\n" );
			this->key=(int*)calloc(size, sizeof(int));
			this->value=(int*)calloc(size, sizeof(int));//verificar el .key y .value ya que son apuntadores
			this->size = 0;
			return *this;
}
