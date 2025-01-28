# Análisis de accidentes en EE.UU

Este proyecto se enfoca en analizar datos de los accidentes ocurridos
en EE.UU entre febrero de 2016 y marzo de 2023. El dataset se obtuvo
en https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents, el
fichero zip con este dataset se debe descomprimir en el directorio
`data/` para que el resto de la aplicación lo reconozca correctamente.

## Arquitectura

El proyecto entero se ejecuta en contenedores de docker, orquestados
con docker compose. A continuación se provee una descripción corta de
cada servicio.

### mongo-db

Este contenedor es responsable de persistir los datos del dataset y
los datos procesados en una base de datos NoSQL. La carga inicial del
dataset y la creación de colecciones se realiza durante la primer
ejecución y se persiste en un volumen de docker.

### mongo-express

Contenedor para depuración, su despliegue es opcional y provee una
interfaz amigable para interactuar con el servicio mongo-db.

### spark

Master del clúster Spark para el procesamiento de datos. Encargado de
coordinar los trabajos de procesamiento y los trabajadores disponibles.

### spark-worker

Trabajador del clúster Spark. El procesamiento de datos se realize en
este servicio.

### dashboard

Provee un tablero web para visualizar los datos procesados.

## Despliegue de la aplicación

El despliegue de la aplicación consta de dos pasos:
- Desplegar los contenedores.
- Ejecutar el procesamiento de datos.

### Desplegar los contenedores

Al tratarse de una aplicación basada en docker compose, ejecutar el
siguiente comando en el root del proyecto es suficiente para
desplegarla:

```sh
docker compose up
```

Si se desea desplegar el contenedor mongo-express para depuración,
basta con agregar `--profile debug` al comando previo:

```sh
docker compose --profile debug up
```

Algunos de los servicios están configurados para crear la imagen del
contenedor al ejecutar `docker compose up`, pero sólo lo harán durante
la primera ejecución. Si hace falta reconstruir las imagenes, se puede
agregar `--build` para forzarlo.

```sh
docker compose up --build
```

Una vez se termina de ejecutar la aplicación, el siguiente comando
eliminará los contenedores:

```sh
docker compose down
```

La base de datos utilizada está configurada para guardar los datos
almacenados en un volumen de docker por lo que, para eliminar
completamente la aplicación, será necesario utilizar -v con el comando
anterior

```sh
docker compose down -v
```

### Ejecutar el procesamiento de datos

Con la aplicación desplegada, el siguiente comando proveerá un shell
en el servicio spark para la ejecución de los trabajos de procesamiento:

```sh
docker compose exec spark bash
```

Desde dentro del contenedor, se pueden ejecutar los scripts de pyspark
para procesar y almacenar los datos de salida en mongo. Los siguientes
son los comandos a ejecutar para todo el procesamiento:

```sh
/scripts/locations.py
/scripts/states.py
/scripts/total.py
/scripts/weather.py
```

## Visualización de datos

Una vez terminada la ejecución de todos los pasos anteriores, la
aplicación desplegará un tablero de visualización en
`http://localhost:8000`.
