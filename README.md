# Practica Módulo Big Data Processing
### Bootcamp Big Data, Machine Learning & IA_Keepcoding
___

Este repositorio contiene el desarrollo de ejercicios de Big Data Processing que abordan conceptos clave del procesamiento distribuido de datos usando Apache Spark y Scala, tanto con DataFrames como con RDDs, aplicando técnicas de filtrado, agregación, joins, UDFs y procesamiento de archivos.
___

## 🎯 Objetivos de la Práctica

- Operaciones con **DataFrames**: realizar transformaciones y acciones básicas.
- Desarrollar y aplicar funciones definidas por el usuario (UDF) para tareas específicas, como determinar la paridad de un número.
- Realizar uniones (joins) entre conjuntos de datos y calcular agregaciones, como el promedio de calificaciones.
- Usar **RDDs** para contar ocurrencias en una colección de palabras.
- Procesar archivos CSV para calcular ingresos totales por producto.

Puedes explorar el desarrollo completo de la práctica en el siguiente enlace: [Practica-BDProcessing](https://github.com/Leticia2512/Practica-Big-Data-Processing/tree/main/Practica-BDProcessing)
___

## 🛠️ Lenguajes y Herramientas 
- **Lenguaje**: Scala
- **Framework**: Apache Spark (incluye Spark SQL y RDDs)
- **Herramientas**: SBT (Scala Build Tool).

___

## ✅ Requisitos para Ejecutar la Práctica 
- Java 8+, Scala y Apache Spark instalados
- SBT para compilar y gestionar dependencias

Para la ejecución arranca una **SparkSession** y un **SparkContext**:

```scala
implicit val spark: SparkSession = SparkSession.builder
  .appName("ExamenPractica")
  .master("local[*]")
  .getOrCreate()
implicit val sc: SparkContext = spark.sparkContext



