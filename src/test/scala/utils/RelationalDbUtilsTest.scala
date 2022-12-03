package utils

import org.apache.spark.sql.functions.{col, monotonically_increasing_id, to_timestamp}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import utils.RelationDbUtilDemo.getData
import utils.RelationalDbUtil.upsertData

class RelationalDbUtilsTest extends AnyFunSuite {
  val spark: SparkSession = SparkIOUtil.getSparkSession

  test("Test for loading data to POSTGRES") {
    val df = spark.read.format("csv").option("header", "true").load("10000 Sales Records.csv")

    val requiredDF = df
      .withColumn(
        "seller_id",
        monotonically_increasing_id()
      ).select("seller_id", "Country", "Order Date", "Ship Date")

    requiredDF.show()

    //    val configs = Map(
    //      "url" -> "jdbc:postgresql://localhost:5432/sparkbootcamp",
    //      "user" -> "ankur",
    //      "password" -> "niki&james",
    //      "database" -> "sparkBootCamp",
    //      "port" -> "5432",
    //      "driver" -> "org.postgresql.Driver"
    //
    //    )
    //    requiredDF.write.format("jdbc").options(
    //      configs
    //    ).option("dbtable", "sales10000").mode(SaveMode.Overwrite).save()
    //
    //    requiredDF.show()
    //    requiredDF.printSchema()

  }

  test("Test reading data from postgres") {
    val configs = Map(
      "url" -> "jdbc:postgresql://localhost:5433/leetcode",
      "user" -> "postgres",
      "password" -> "postgres",
      "database" -> "leetcode",
      "port" -> "5433",
      "driver" -> "org.postgresql.Driver",
      "dbtable" -> "orders_586",
      "numPartitions" -> "2",
      "partitionColumn" -> "order_number",
      "lowerBound" -> "1",
      "upperBound" -> "5"
    )

    val extractedDF = getData(spark, configs)
    extractedDF.explain()
    extractedDF.show()
    extractedDF.printSchema()
  }

  test("Upsert logic test"){
    import spark.implicits._

    val data1 = Seq(
      (1, "India", "07-01-2019 12 01 19 406"),
      (2, "USA", "06-24-2019 12 01 19 406"),
      (3, "UK", "11-16-2019 16 44 55 406"),
      (4, "Sri Lanka", "11-16-2019 16 50 59 406"),
      (5, "India", "07-01-2019 12 01 19 406")
    )

    val oldDf = data1.toDF("seller_id", "country", "creation_date").withColumn(
      "creation_date", to_timestamp(col("creation_date"),"MM-dd-yyyy HH mm ss SSS")
    )

    oldDf.show()
    oldDf.printSchema()

    val data2 = Seq(
      (2, "USA", "06-24-2019 12 01 19 406"),
      (3, "UK", "11-16-2019 16 44 55 406"),
      (4, "Bhutan", "11-07-2022 16 50 59 406"),
      (5, "India", "07-01-2019 12 01 19 406"),
      (6, "Australia", "11-07-2022 16 50 59 406"),
      (7, "India", "11-07-2022 16 50 59 406")
    )

    val newDf = data2.toDF("seller_id", "country", "creation_date").withColumn(
      "creation_date", to_timestamp(col("creation_date"),"MM-dd-yyyy HH mm ss SSS")
    )

    newDf.show()
    newDf.printSchema()
//
    val requiredDF = upsertData(oldDf, newDf)

  }
}
