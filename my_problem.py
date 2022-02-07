#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb  6 11:51:15 2022

@author: jesus octavio raboso
"""

import sys 
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pylab import *

from pyspark import SparkContext
from pyspark.sql import SparkSession
import json


print ('Number of arguments: %i arguments' % len(sys.argv))
print ('Argument List:' + str(sys.argv))

print(str(sys.argv[1]))
print(str(sys.argv[2]))

sc = SparkContext()

"""
Leemos las rdds del mejor y peor mes de acuerdo a los resultados obtenidos en la parte anterior para los viajes filtrados, pues sólo nos interesa trabajar con los viajes que hemos considerado válidos. El peor mes es diciembre de 2018 y el mejor mes es junio de 2019.
"""
rdd_worst = sc.textFile(str(sys.argv[1]))
rdd_best = sc.textFile(str(sys.argv[2]))

"""
Definimos un mapper, que aplicaremos a la hora de leer los archivos json relativos al peor y mejor mes, para quedarnos únicamente con los datos que van a ser necesarios:

    Estación de origen
    Estación de destino
    Tiempo de viaje

"""

def mapper(line):
    data = json.loads(line)
    origin = data['idunplug_station']
    destination = data['idplug_station']
    travel_time = data['travel_time']
    return origin, destination, travel_time


"""
Filtramos los viajes por aquellos que duran más de 30 segundos y sus estaciones de origen y destino son distintas. Cada elemento de las rdd rdd_worst_filtered (para el peor mes) y rdd_best_filtered (para el mejor mes) será de la forma:
"""

rdd_worst_filtered = rdd_worst.map(mapper).\
                     filter(lambda x: x[0]!=x[1] and x[2]>30)

rdd_best_filtered = rdd_best.map(mapper).\
                    filter(lambda x: x[0]!=x[1] and x[2]>30)

"""
### PROBLEMA 1: DURACIÓN MEDIA DE LOS VIAJES



Vamos a calcular la duración media de los viajes que parten de una estación. Recordamos que el tiempo se guarda en segundos.

En primer lugar, lo hacemos para el peor mes.

* Creamos un diccionario en el que las claves son las estaciones de origen y el valor, las veces que aparece en la rdd de viajes filtrados. Es decir, el número de viajes que parten de ella. Será relevente para calcular el tiempo medio.
* Nos quedamos con la estación de origen y el tiempo de cada viaje, los reducimos según la clave mediante la suma de valores. Hallamos la media. Los ordenamos según el número de estacion de menor a mayor.
"""

dict_worst = dict(rdd_worst_filtered.map(lambda x: (x[0], x[2])).countByKey())

rdd_average_time_worst = rdd_worst_filtered.map(lambda x: (x[0],x[2])).\
                        reduceByKey(lambda x,y: x+y).\
                        map(lambda x: (x[0], x[1]/dict_worst[x[0]])).\
                        sortByKey()


"""
Repetimos con el meor mes
"""

dict_best = dict(rdd_best_filtered.map(lambda x: (x[0], x[2])).\
                              countByKey())

rdd_average_time_best = rdd_best_filtered.map(lambda x: (x[0],x[2])).\
                        reduceByKey(lambda x,y: x+y).\
                        map(lambda x: (x[0], x[1]/dict_best[x[0]])).\
                        sortByKey()

"""
Para hacer mejor un análisis de los datos lo que hacemos es representar los valores obtenidos en una gráfica. Para ello, lo primero que hacemos es crear un Data Frame y un Panda para poder obtener las columnas y hacer la gráfica comparativa.
"""

rdd_average_time_comp = rdd_average_time_best.join(rdd_average_time_worst).sortByKey().map(lambda x: (x[0], x[1][0], x[1][1], x[1][0]-x[1][1]))

spark = SparkSession.builder.getOrCreate()
dft = spark.createDataFrame(rdd_average_time_comp, schema = ['Station number', 'Average travel time best month','Average travel time worst month', 'Difference best month-worst month' ])
pt = dft.toPandas()
stations = list(pt.iloc[:,0])
bm = list(pt.iloc[:,1]) #best month
wm = list(pt.iloc[:,2]) #worst month
d = list(pt.iloc[:,3]) #difference
p1 = plot(stations, bm,c='b',label='Best month',mfc='y',ls='-',marker='.')
p2 = plot(stations, wm,c='orange',label='Worst month',mfc='y',ls='-',marker='.')
p3 = plot(stations, d,c='r',label='Difference',mfc='y',ls='-',marker='.')
xlabel("Station number")
ylabel("Average time of trips")
legend(loc='best', fontsize = 'small')
title('Comparative average time')
plt.xticks(rotation=45)
plt.rcParams["figure.figsize"] = [16,9] #Ejecutar dos veces para obtener este tamaño
show()