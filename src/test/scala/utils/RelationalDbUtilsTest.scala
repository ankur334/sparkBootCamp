package utils

import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import utils.RelationDbUtilDemo.getData

class RelationalDbUtilsTest extends AnyFunSuite {
  val spark: SparkSession = SparkIOUtil.getSparkSession

  test("Test for loading data to POSTGRES") {
    val df = spark.read.format("csv").option("header", "true").load("10000 Sales Records.csv")

    val requiredDF = df
      .withColumn(
        "seller_id",
        monotonically_increasing_id()
      ).withColumn(
      "Ship Date",
      col("Order Date").cast("timestamp"))
      .withColumn("Order Date",
        col("Order Date").cast("timestamp"))

    val configs = Map(
      "url" -> "jdbc:postgresql://localhost:5432/sparkbootcamp",
      "user" -> "ankur",
      "password" -> "niki&james",
      "database" -> "sparkBootCamp",
      "port" -> "5432",
      "driver" -> "org.postgresql.Driver"

    )
    requiredDF.write.format("jdbc").options(
      configs
    ).option("dbtable", "sales10000").mode(SaveMode.Overwrite).save()

    requiredDF.show()
    requiredDF.printSchema()

  }

  test("Test reading data from postgres") {
    val configs = Map(
      "url" -> "jdbc:postgresql://localhost:5432/sparkbootcamp",
      "user" -> "ankur",
      "password" -> "niki&james",
      "database" -> "sparkBootCamp",
      "port" -> "5432",
      "driver" -> "org.postgresql.Driver",
      "dbtable" -> "sales10000",
      "numPartitions" -> "10",
      "partitionColumn" -> "seller_id",
      "lowerBound" -> "1",
      "upperBound" -> "10000"
    )

    val extractedDF = getData(spark, configs)
    extractedDF.explain()
    extractedDF.show()
    extractedDF.printSchema()
  }
}
