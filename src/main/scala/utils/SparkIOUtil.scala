package utils

import org.apache.spark.sql.SparkSession

object SparkIOUtil {

  def getSparkSession: SparkSession = {
      val spark:SparkSession = SparkSession.builder()
        .master("local[*]").appName("SparkByExamples.com")
        .getOrCreate()

      spark
    }
}
