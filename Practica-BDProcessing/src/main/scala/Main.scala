import org.apache.spark.sql.SparkSession
import examen.spUtils.SparkUtils.runSparkSession


object Main {
 def main(args: Array[String]): Unit = {
   implicit val spark: SparkSession = runSparkSession("keepcoding")

   spark.read.csv("/Users/leticm03/BD15/Big Data Processing/Practica-BDProcessing/src/test/resources/ventas.csv").show(false)

  }
}
