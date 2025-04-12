package utils

import examen.spUtils.SparkUtils
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.reflect.io.Directory

trait SparkSessionTestWrapper {
  FileUtils.deleteDirectory(new File("metastore_db"))
  new Directory(new File("src/test/resources/tmp")).deleteRecursively()

  implicit val spark: SparkSession = SparkUtils.runSparkSession("spark-test")

  spark.sparkContext.setLogLevel("WARN")
}
abstract class TestInit extends AnyFlatSpec with Matchers with BeforeAndAfterAll with SparkSessionTestWrapper {

  lazy val testPath: String = "src/test/resources"

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  def newDf(datos: Seq[Row], schema: StructType): DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(datos), schema)

  def setNullableStateForAllColumns(df: DataFrame, nullable: Boolean = true): DataFrame =
    df.sqlContext.createDataFrame(df.rdd, StructType(df.schema.map {
      case StructField(name, dataType, _, metadata) =>
        StructField(name, dataType, nullable = nullable, metadata)
    }))

  def checkDf(expected: DataFrame, actual: DataFrame): Unit = {
    expected.schema.toString() should be(actual.schema.toString())
    expected.collectAsList() should be(actual.collectAsList())
  }

  def checkDfIgnoreDefault(expected: DataFrame, actual: DataFrame): Unit = {
    setNullableStateForAllColumns(expected).schema.toString() should be(
      setNullableStateForAllColumns(actual).schema.toString()
    )
    expected.collectAsList() should be(actual.collectAsList())
  }
}