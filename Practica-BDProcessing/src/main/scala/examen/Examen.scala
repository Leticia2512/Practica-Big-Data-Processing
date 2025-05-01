package examen

import org.apache.spark.rdd._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, udf}

object Examen {
  /** Ejercicio 1:
   * Crea un DataFrame a partir de una secuencia de tuplas que contenga información sobre
   * estudiantes (nombre, edad, calificación).
   *
   * Realiza las siguientes operaciones:
   *
   * • Muestra el esquema del DataFrame.
   *
   * • Filtra los estudiantes con una calificación mayor a 8.
   *
   * • Selecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente.
   */
  def topEstudiantes(estudiantes: DataFrame): (DataFrame) = {
    estudiantes.printSchema()

    val estudiantesFiltrados = estudiantes.filter(col("calificacion") > 8)

    val estudiantesOrdenados = estudiantesFiltrados
      .select("nombre", "calificacion")
      .orderBy(col("calificacion").desc)

    estudiantesOrdenados
  }

  /** Ejercicio 2: UDF (User Defined Function)
   *
   * Pregunta: Define una función que determine si un número es par o impar.
   *
   * Aplica esta función a una columna de un DataFrame que contenga una lista de números.
   */

  def Paridad(df: DataFrame, columna: String): DataFrame = {
    val numParOImpar = (numero: Int) => if (numero % 2 == 0) "par" else "impar"
    val udfParidad = udf(numParOImpar)

    val dfConParidad = df.withColumn("paridad", udfParidad(col(columna)))

    dfConParidad

  }

  /** Ejercicio 3: Joins y agregaciones
   *
   * Pregunta: Dado dos DataSet,
   * uno con información de estudiantes (id, nombre)
   * y otro con calificaciones (id_estudiante, asignatura, calificacion),
   * realiza un join entre ellos y calcula el promedio de calificaciones por estudiante.
   */

  def calcularPromedioEstudiante(estudiantes: DataFrame, calificaciones: DataFrame): DataFrame = {
    val estudiantesCalificaciones = estudiantes.join(calificaciones, estudiantes("id") === calificaciones("id_estudiante"))

    val promedioEstudiante = estudiantesCalificaciones
      .groupBy("id", "nombre")
      .agg(avg("calificacion").as("promedio_calificacion"))

    promedioEstudiante
  }

  /** Ejercicio 4: Uso de RDDs
   *
   * Crea un RDD a partir de una lista de palabras y cuenta la cantidad de ocurrencias de cada palabra.
   */

  def ocurrenciasPalabras(lista: List[String])(implicit sc: SparkContext): RDD[(String, Int)] = {
    val rddOcurrencias = sc.parallelize(lista)
      .map(palabra => (palabra, 1))
      .reduceByKey(_ + _)

    rddOcurrencias
  }

  /**
   * Ejercicio 5: Procesamiento de archivos
   *
   * Carga un archivo CSV que contenga información sobre
   * ventas (id_venta, id_producto, cantidad, precio_unitario)
   * y calcula el ingreso total (cantidad * precio_unitario) por producto.
   */

  def readCSV(inputPath: String, delimiter: String = ",")(implicit spark: SparkSession): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", delimiter)
      .csv(inputPath)
  }

  def calcularIngresoTotal(venta: DataFrame, outputPath: String): Unit = {
    val csvIngresoTotal = venta
      .withColumn("ingreso", col("cantidad").cast("double") * col("precio_unitario"))
      .groupBy("id_producto")
      .agg(sum("ingreso").as("total_ingreso"))

    val newOutputPath = outputPath.replace(".csv", "2.csv")

    csvIngresoTotal.write
      .option("header", "true")
      .csv(newOutputPath)
  }

}
