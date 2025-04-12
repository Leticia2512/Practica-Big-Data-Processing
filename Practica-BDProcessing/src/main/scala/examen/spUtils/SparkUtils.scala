package examen.spUtils

import org.apache.spark.sql.SparkSession

object SparkUtils {

  def runSparkSession(name: String): SparkSession = SparkSession.builder()
    .appName(name)
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

}
