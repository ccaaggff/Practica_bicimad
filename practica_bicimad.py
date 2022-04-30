"OBSERVACIÓN: La práctica toma los archivos de una carpeta llamada BiciMAD con los archivos json necesarios."
"Nosotros usamos los de enero, febrero y marzo de 2018 para nuestro ejercicio"
"Como por el peso no nos dejaba subirlo a github, añadimos los archivos comprimidos que deberán descomprimirse"
"y poner en una carpeta bajo el nombre BiciMAD. Si en esa carpeta se cargan más datos, el programa los tendrá en cuenta."

from pyspark import SparkContext, SparkConf
import json, os
from pprint import pprint 
from numpy import mean


def data(line): #Recopilación de datos de los arcivhos
    data = json.loads(line)
    u_t = data['user_type'] 
    u_c = data['user_day_code']
    start = data['idunplug_station']
    end = data['idplug_station']
    time = data['travel_time']
    
    return u_t, u_c, start, end, time  


def reducir(tuple1, tuple2):
    n = min(len(tuple1), len(tuple2))
    sol = ()
    for i in range(n):
        sol = sol + (tuple1[i] + tuple2[i],)
    return sol

def tuple_to_str(tuple):
    return '(' + ','.join([str(elem) for elem in tuple]) + ')'

def process(list_or,list_te):
    list_te = [(elem, 0) for elem in list_te]
    list_or = sorted(list_or + list_te)
    aux = []
    for i in range(len(list_or)-1):
        if list_or[i][0] != list_or[i+1][0]:
            aux.append(list_or[i][1])
    aux.append(list_or[len(list_or)-1][1])
    return aux
        

def rdd_recopilado(rdd_init): #veces usada y tiempo medio
    rdd = rdd_init.filter(lambda x: x[0]==1).\
        map(lambda x: (x[2:4], (x[4],1))).\
        reduceByKey(reducir).\
        mapValues(lambda x: (x[1], x[0]/x[1])).\
        sortByKey(ascending=True)       
    return rdd
    


def main():
    conf = SparkConf().setAppName("Rutas")
    sc = SparkContext(conf=conf)
    direct = os.path.abspath('BiciMAD')
    rdd_final = []
    rdd_final = sc.parallelize(rdd_final)
    files = []
    for filename in os.listdir(direct):
        if  filename.endswith(".json"):
            print(filename)
            files.append(filename[:6])
                
            rdd_init = sc.textFile(direct+'/'+filename).map(data) #Nos quedamos con los datos que vamos a usar
            rdd_init = rdd_recopilado(rdd_init).map(lambda x: (x[0], (filename[:6], x[1][0]))) #Guardamos las rutas con el número de usos
            pprint(rdd_init.take(10))
            rdd_final = rdd_final.union(rdd_init) #Lo únimos en un RDD
        else:
            pass
    rdd_final1 = rdd_final.\
        groupByKey().\
        mapValues(lambda x: process(list(x), files)).\
        sortByKey(ascending=True).\
        map(lambda x : (tuple_to_str(x[0]),)+tuple(x[1]))
#    pprint(rdd_final1.take(10))
    
    
    rdd_media = rdd_final.\
        groupByKey().\
        mapValues(lambda x: process(list(x), files)).\
        mapValues(mean).\
        sortBy(lambda x: x[1], ascending=False)
    print("Medias ordenadas:")
    pprint(rdd_media.take(10)) #Nos da las medias de uso en los meses. Tomamos los dos primeros y analizamos. 

                    
if __name__ == "__main__":
    main()    
                