### Práctica 1. César de Diego Morales 
#Implementar un merge concurrente:
#- Tenemos NPROD procesos que producen números no negativos de forma
#creciente. Cuando un proceso acaba de producir, produce un -1. 
#Cada proceso almacena el valor almacenado en una variable compartida con el consumidor,
#un -2 indica que el almacén está vacío.

#- Hay un proceso merge que debe tomar los números y almacenarlos de
#forma creciente en una única lista (o array). El proceso debe esperar a que
#los productores tengan listo un elemento e introducir el menor de
#ellos.

#- Se debe crear listas de semáforos. Cada productor solo maneja los
#sus semáforos para sus datos. El proceso merge debe manejar todos los
#semáforos.

#- OPCIONALMENTE: se puede hacer un búffer de tamaño fijo de forma que
#los productores ponen valores en el búffer.

from multiprocessing import Process
from multiprocessing import Semaphore,Lock, BoundedSemaphore
from multiprocessing import current_process, Manager
from multiprocessing import Value, Array
from random import randint,random
from time import sleep

N =  20 #Número de vueltas 
NPROD = 5 #Número de productores

def delay(factor = 3):
    sleep(random()/factor)

def minimo_producto(buffer): 
    try:
        lista_prod = []
        for i in range(len(buffer)): # Creamos una lista con los elementos de valor no negativo. 
                                     # Los elementos de valor negativo se pondran como el maximo + 1 para que no se escoja como minimo  
            if buffer[i] < 0:
                lista_prod.append(max(buffer)+1) 
            else:
                lista_prod.append(buffer[i])
        minimo = min(lista_prod)
        posicion = lista_prod.index(minimo)
    finally:
        return minimo, posicion  #devolvemos el minimo y el productor al que corresponde

def buffer_no_vacio(buffer): #Para comprobar que el buffer no esta vacío
    suma = 0
    for productor in range(NPROD):
        if buffer[productor] != -1: # Se puede seguir consumiendo
            suma += 1
    return suma != 0
            
def producer(buffer,empty,non_empty,valores_cons):
    for vuelta in range(N):
        print(f"producer {current_process().name} produciendo")
        pro = int(current_process().name.split('_')[1]) # El productor concreto 
        empty[pro].acquire()  # Hacemos un wait en el sem correspondiente para decir que se puede producir
        producto_nuevo = valores_cons.value + randint(0,10)
        buffer[pro] = producto_nuevo #Incluimos los nuevos productos en el buffer
        delay()
        non_empty[pro].release() #Hacemos un signal en el sem correspondiente para decir que se puede consumir
        print(f"producer {current_process().name} almacenado {producto_nuevo}")
        print (buffer[:])
    pro = int(current_process().name.split('_')[1])
    print (f"productor {pro} ha terminado")
    empty[pro].acquire() # Hacemos un wait para indicar que se va a producir el -1, es decir para decir que ya ha terminado
    buffer[pro] = -1
    non_empty[pro].release() #Hacemos un signal para indicar que de hay no se consume más    
    
def consumer(buffer,empty,non_empty,prods_cons,valores_cons):
    for productor in range(NPROD):
        non_empty[productor].acquire() #Hacemos un wait para indicar que vamos a consumir 
    while buffer_no_vacio(buffer):
        producto, posicion = minimo_producto(buffer) #Tomamos el elemento minimo que se pueda consumir
        prods_cons.append(producto)
        buffer[posicion] = -2
        valores_cons.value = producto
        print(f"consumiendo {producto} del productor {posicion}")
        empty[posicion].release()# Hacemos un signal para indicar en que esa posicion se deja un hueco para que se produzca
        non_empty[posicion].acquire() 
        
def main():
    buffer = Array("i",NPROD) #Almacen
    valores_cons = Value("i",0) #Value que contiene el valor del ultimo producto producido, que ya se ha consumido
    for i in range(NPROD):
        buffer[i] = -2 #Representamos con un -2 que no hay nada producido aun
    print("Almacén inicial",buffer[:])   
    empty = [Lock() for i in range(NPROD)] #Semáforo empty del productor
    non_empty = [Semaphore(0) for i in range(NPROD)] #Semáforo non_empty del consumidor
    manager = Manager()
    prods_cons = manager.list() #Lista donde se guardan las consumiciones realizadas
    prodlst = [Process(target=producer,
                        name=f'prod_{i}',
                        args = (buffer,empty,non_empty,valores_cons)) 
               for i in range(NPROD) ] #Productores
    consumidor = Process(target=consumer,
                           name="consumidor_único", 
                           args = (buffer,empty,non_empty,prods_cons,valores_cons)) #Consumidores
    procesos = prodlst + [consumidor]
    for p in procesos:
        p.start()
    for p in procesos:
        p.join()
    delay()    
    print("\n \n \n")
    print("buffer final",buffer[:])
    print("lista de consumiciones", prods_cons[:])
    
if __name__ == "__main__":
    main()

