from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

from helpers import *

path_containers = 'data/containers.csv'

def ejercicio_0(sc, path_resultados):
  lineas = sc.parallelize(range(10)).collect()
  with open(path_resultados(0), 'w') as f:
    f.write("{}\n".format(",".join([str(s) for s in lineas])))
  return lineas

# Ejercicio 1. Leer el archivo data/containers.csv y contar el número de líneas.
def ejercicio_1(sc, path_resultados):
  # COMPLETAR CÓDIGO AQUÍ
  # Devolver número de líneas

  #leemos em fichero
  rdd_containers = sc.textFile(path_containers)

  #contamos el numero de lineas
  lineas_rdd_containers = rdd_containers.count()

  #creamos fichero ressultado_1
  with open(path_resultados(1), 'w')as f:
    f.write(str(lineas_rdd_containers))
  return lineas_rdd_containers
  #return 0

# Ejercicio 2. Leer el archivo data/containers.csv y filtrar aquellos
# contenedores cuyo ship_imo es DEJ1128330 y el grupo del contenedor es 22P1.
# Guardar los resultados en un archivo de texto en resultados/resutado_2.
def ejercicio_2(sc, path_resultados):
  # COMPLETAR CÓDIGO AQUÍ
  # Guardar en resultados/resultado_2. La función path_resultados devuelve
  # la ruta donde se van a guardar los resultados, para que los tests puedan
  # ejecutar de forma correcta. Por ejemplo, path_resultados(2) devuelve la
  # ruta para el ejercicio 2, path_resultados(3) para el 3, etc.
  # Devolver rdd contenedores filtrados:
  # return rdd.collect()

  #leemos el fichero
  txt = sc.textFile(path_containers)
  
  #emparejamos los datos con las cabeceras
  no_header = txt.filter(lambda s: not s.startswith('ship_imo'))
  parsed = no_header.map(parse_container)
  
  #filtramos rdd
  rdd = parsed.filter(lambda i: i.ship_imo == 'DEJ1128330' and i.container_group =='22P1')
  
  #guardamos el rdd
  rdd.saveAsTextFile(path_resultados(2))

  return rdd.collect()
  #pass

# Ejercicio 3. Leer el archivo data/containers.csv y convertir a formato
# Parquet. Recuerda que puedes hacer uso de la funcion parse_container en
# helpers.py tal y como vimos en clase. Guarda los resultados en
# resultados/resultado_3.
def ejercicio_3(sc, path_resultados):
  # COMPLETAR CÓDIGO AQUÍ
  # Guardar resultados y devolver DataFrame (return df)
  
  sq = SQLContext(sc)
  
  #leemos el fichero
  txt = sc.textFile(path_containers)
  
  #emparjamos los datos con las cabeceras
  no_header = txt.filter(lambda s: not s.startswith('ship_imo'))
  parsed = no_header.map(parse_container)
  

  #creamos el dataframe
  csv_source = parsed.map(lambda c: Row(**dict(c._asdict())))
  #containerSchema = sq.createDataFrame(csv_source)
  df = sq.createDataFrame(csv_source)
  #seleccionamos los datos del dataframe
  #containerSchema.createOrReplaceTempView('container')
  #df = sq.sql("SELECT * FROM container")

  #guardamos el dataframe en un fichero 
  df.write.mode('overwrite').parquet(path_resultados(3))

  return df

# Ejercicio 4. Lee el archivo de Parquet guardado en el ejercicio 3 y filtra
# los barcos que tienen al menos un contenedor donde la columna customs_ok es
# igual a false. Extrae un fichero de texto una lista con los identificadores
# de barco, ship_imo, sin duplicados y ordenados alfabéticamente.
def ejercicio_4(sc, path_resultados):
  # COMPLETAR CÓDIGO AQUÍ
  # Guardar resultados y devolver DataFrame (return df)
  
  sq = SQLContext(sc)
  
  #cargamos el parquet
  #df_parquet = sq.read.load('data/containers_tiny.parquet')
  df_parquet = sq.read.load('resultados/resultado_3')
  df_parquet.createOrReplaceTempView("container")



  #seleccionamos los datos que queremos en el dataframe
  df = sq.sql("SELECT ship_imo FROM container WHERE customs_ok = FALSE GROUP BY ship_imo ORDER BY ship_imo ASC")


  #guardamos el datraframe en formato json
  df.coalesce(1).write.format('json').save(path_resultados(4))


  return df

# Ejercicio 5. Crea una UDF para validar el código de identificación del
# contenedor container_id. Para simplificar la validación, daremos como
# válidos aquellos códigos compuestos de 3 letras para el propietario, 1
# letra para la categoría, 6 números y 1 dígito de control. Devuelve un
# DataFrame con los campos: ship_imo, container_id, propietario, categoria,
# numero_serie y digito_control.
def ejercicio_5(sc, path_resultados):
  # COMPLETAR CÓDIGO AQUÍ
  # Guardar resultados y devolver DataFrame (return df)
  sq = SQLContext(sc)
  
  #leemos el fichero
  txt = sc.textFile(path_containers)
  
  #emparjamos los datos con las cabeceras
  no_header = txt.filter(lambda s: not s.startswith('ship_imo'))
  parsed = no_header.map(parse_container)
  

  #creamos el dataframe
  csv_source = parsed.map(lambda c: Row(**dict(c._asdict())))
  df_sf = sq.createDataFrame(csv_source)

  df_sf.createOrReplaceTempView("container")


  sq.registerFunction('en_propietario', lambda x:(str(x)[:3]))
  sq.registerFunction('en_categoria', lambda x:(str(x)[3:4]))
  sq.registerFunction('en_numero_serie', lambda x:(str(x)[4:10]))
  sq.registerFunction('en_digito_control', lambda x:(str(x)[10:11]))

  df = sq.sql("SELECT en_propietario(container_id) propietario, en_categoria(container_id) categoria,\
   en_numero_serie(container_id) numero_serie, en_digito_control(container_id) digito_control, container_id, ship_imo FROM container")

  #guardamos el datraframe en formato json
  df.coalesce(1).write.format('json').save(path_resultados(5))


  return df

# Ejercicio 6. Extrae una lista con peso total de cada barco, `net_weight`,
# sumando cada contenedor y agrupado por los campos `ship_imo` y `container_group`.
# Devuelve un DataFrame con la siguiente estructura:
# `ship_imo`, `ship_name`, `container_group`, `total_net_weight`.
def ejercicio_6(sc, path_resultados):
  # COMPLETAR CÓDIGO AQUÍ
  # Guardar resultados y devolver DataFrame (return df)

  sq = SQLContext(sc)
    #leemos el fichero
  txt = sc.textFile(path_containers)
  
  #emparjamos los datos con las cabeceras
  no_header = txt.filter(lambda s: not s.startswith('ship_imo'))
  parsed = no_header.map(parse_container)
  

  #creamos el dataframe
  csv_source = parsed.map(lambda c: Row(**dict(c._asdict())))
  containerSchema = sq.createDataFrame(csv_source)

  containerSchema.createOrReplaceTempView('container')

  #seleccionamos los datos que queremos en el dataframe
  df = sq.sql("SELECT container_group, ship_imo, ship_name, sum(net_weight) total_net_weight FROM container GROUP BY ship_imo, container_group, ship_name")

  #guardamos el datraframe en formato csv
  df.toPandas().to_csv(path_resultados(6), sep=';', index=False)
  #df.write.options("sep",";").option("header","true").csv(path_resultados(6))
  return df

# Ejercicio 7. Guarda los resultados del ejercicio anterior en formato Parquet.
def ejercicio_7(sc, path_resultados):
  # COMPLETAR CÓDIGO AQUÍ
  # Guardar resultados y devolver DataFrame (return df)
  sq = SQLContext(sc)
  
  #leemos los datos del csv creado en el ejercicio 6
  rdd = sc.textFile('resultados/resultado_6')

  #Mapeamos el RDD
  mappedRdd= rdd.map(lambda x:x.split(";"))
  mappedRddWithoutHeader=mappedRdd.filter(lambda x:x[0]!='Container' and x[0]!='String')
  
  headerRdd=sc.textFile("resultados/resultado_6").map(lambda x:x.split(";")).take(1)[0]
  
  # Ceamos el DF del RDD
  df=mappedRddWithoutHeader.toDF(headerRdd)
  
  #guardamos el datraframe en formato parquet
  df.write.mode('overwrite').parquet(path_resultados(7))
  


  return df

def main():
  sc = SparkContext('local', 'practicas_spark')
  pr = definir_path_resultados('./resultados')
  ejercicio_0(sc, pr)
  ejercicio_1(sc, pr)
  ejercicio_2(sc, pr)
  ejercicio_3(sc, pr)
  ejercicio_4(sc, pr)
  ejercicio_5(sc, pr)
  ejercicio_6(sc, pr)
  ejercicio_7(sc, pr)

if __name__ == '__main__':
  main()

