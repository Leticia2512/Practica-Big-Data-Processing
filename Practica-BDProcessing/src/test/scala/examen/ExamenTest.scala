package examen

import examen.Examen._
import org.apache.spark.sql.{DataFrame, SparkSession, Dataset}
import examen.spUtils.SparkUtils
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import utils.TestInit

class ExamenTest extends TestInit {
  implicit val sc: SparkContext = spark.sparkContext
  import spark.implicits._

  val esquema = StructType(Array(
    StructField("nombre", StringType, false),
    StructField("edad", IntegerType, false),
    StructField("calificacion", DoubleType, false)
  ))

  val estudiantesData = newDf(Seq(
      Row("Ana", 22, 9.2),
      Row("Luis", 20, 7.5),
      Row("Carlos", 23, 8.3),
      Row("Marta", 21, 10.0),
      Row("Juan", 22, 6.8),
      Row("Sonia", 20, 5.7),
      Row("Ricardo", 23, 8.7),
      Row("David", 23, 6.2),
      Row("Patricia", 24, 7.8),
      Row("Cristina", 21, 9.6),
      Row("Pablo", 24, 7.7),
      Row("Julia", 24, 8.4),
      Row("Susana", 22, 6.5)
    ),
  esquema
  )

  "Función calificacionEstudiantes" should "debe devolver el esquema, filtrar y ordenar correctamente" in {

    estudiantesData.schema shouldBe esquema

    val dfObtenido = topEstudiantes(estudiantesData)

    val esquemaDFmodificado = StructType(Array(
      StructField("nombre", StringType, false),
      StructField("calificacion", DoubleType, false)
    ))

    val dfEsperado = newDf(Seq(
      Row("Marta", 10.0),
      Row("Cristina", 9.6),
      Row("Ana", 9.2),
      Row("Ricardo", 8.7),
      Row("Julia", 8.4),
      Row("Carlos", 8.3)
    ),
    esquemaDFmodificado
    )

    dfObtenido.collect() should contain theSameElementsAs dfEsperado.collect()

    println("Data Frame con calificación mayor a 8 y ordenado de manera descendente:")
    dfObtenido.show(false)
  }

  "función Paridad" should "agregar columna paridad" in {
    import spark.implicits._
    val inputData = Seq(
      (2), (25), (15), (42), (7), (38), (28), (97), (14), (19)
    ).toDF("numeros")

    val dfParidadEsperado = Seq(
      (2, "par"), (25, "impar"), (15, "impar"), (42, "par"),
      (7, "impar"), (38, "par"), (28, "par"), (97, "impar"),
      (14, "par"), (19, "impar")
    ).toDF("numeros", "paridad_numeros")

    val dfParidadObtenido = Paridad(inputData, "numeros")

    dfParidadObtenido.collect().toSet shouldBe dfParidadEsperado.collect().toSet

    println("DataFrame con paridad de los números:")
    dfParidadObtenido.show(false)
  }


    "Función calcularPromedioEstudiante" should "calcular promedio por estudiante" in {
      import spark.implicits._

      val estudiantes = Seq(
        (1, "Ana"), (2, "Luis"), (3, "María"), (4, "Carlos"), (5, "Sofía"),
        (6, "Pedro"), (7, "Lucía"), (8, "Javier"), (9, "Marta"), (10, "Diego"),
        (11, "Elena"), (12, "Raúl"), (13, "Clara"), (14, "Andrés"), (15, "Paula"),
        (16, "Fernando"), (17, "Isabel"), (18, "Manuel"), (19, "Carmen"), (20, "Rosa")
      ).toDF("id", "nombre")

      val calificaciones = Seq(
        (1, "Matemáticas", 8.5), (2, "Matemáticas", 6.0), (3, "Matemáticas", 7.8),
        (4, "Matemáticas", 6.8), (5, "Matemáticas", 9.2), (6, "Historia", 5.3),
        (7, "Historia", 8.5), (8, "Historia", 7.0), (9, "Historia", 7.5),
        (10, "Historia", 9.5), (11, "Ciencias", 8.8), (12, "Ciencias", 6.2),
        (13, "Ciencias", 9.1), (14, "Ciencias", 7.7), (15, "Ciencias", 8.9),
        (16, "Inglés", 9.3), (17, "Inglés", 5.8), (18, "Inglés", 8.4),
        (19, "Inglés", 7.7), (20, "Inglés", 9.0)
      ).toDF("id_estudiante", "asignatura", "calificacion")

      val dfPromedioEsperado = Seq(
        (1, "Ana", 8.5), (2, "Luis", 6.0), (3, "María", 7.8), (4, "Carlos", 6.8), (5, "Sofía", 9.2),
        (6, "Pedro", 5.3), (7, "Lucía", 8.5), (8, "Javier", 7.0), (9, "Marta", 7.5), (10, "Diego", 9.5),
        (11, "Elena", 8.8), (12, "Raúl", 6.2), (13, "Clara", 9.1), (14, "Andrés", 7.7), (15, "Paula", 8.9),
        (16, "Fernando", 9.3), (17, "Isabel", 5.8), (18, "Manuel", 8.4), (19, "Carmen", 7.7), (20, "Rosa", 9.0)
      ).toDF("id", "nombre", "promedio_calificacion")

      val dfPromedioObtenido = calcularPromedioEstudiante(estudiantes, calificaciones)

      val schemaEsperado = StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("nombre", StringType, nullable = true),
        StructField("promedio_calificacion", DoubleType, nullable = true)
      ))

      dfPromedioObtenido.schema should be(schemaEsperado)
      dfPromedioEsperado.collect() should contain theSameElementsAs dfPromedioObtenido.collect()

      println("DataFrame con el promedio de la calificación por estudiante:")
      dfPromedioObtenido.show(false)
    }


    "Función ocurrenciasPalabras" should "contar ocurrencias de palabras de una lista" in {

      val listaPalabras = List("girasol", "amapola", "jazmin", "amapola", "amapola", "jazmin", "lavanda",
        "rosa", "lavanda", "girasol", "girasol", "tulipan", "rosa")

      val resultadoObtenido = ocurrenciasPalabras(listaPalabras)

      val resultadoEsperado = Map("girasol" -> 3, "amapola" -> 3, "jazmin" -> 2, "lavanda" -> 2, "rosa" -> 2, "tulipan" -> 1)

      resultadoObtenido.collect().toMap shouldBe resultadoEsperado

      println("Resultado Obtenido ocurrencias por palabra:")
      resultadoObtenido.collect().toMap.foreach(println)

    }


  "La función IngresoTotalProducto" should "calcular los ingresos totales por producto" in {

    val inputPath = "src/test/resources/ventas.csv"
    val dfVentas = readCSV(inputPath)

    calcularIngresoTotal(dfVentas, "src/test/resources/ventas.csv")

    val outputPath = "src/test/resources/ventas2.csv"

    val resultadoObtenido = readCSV(outputPath)

    val resultadoEsperado = Seq(
      (101, 460.0),
      (102, 405.0),
      (103, 280.0),
      (104, 800.0),
      (105, 570.0),
      (106, 425.0),
      (107, 396.0),
      (108, 486.0),
      (109, 540.0),
      (110, 494.0)
    ).toDF("id_producto", "total_ingreso")

    resultadoObtenido.sort("id_producto").collect() shouldBe resultadoEsperado.sort("id_producto").collect()

    println("DataFrame con el ingreso total por producto:")
    resultadoObtenido.show(false)
  }

}




