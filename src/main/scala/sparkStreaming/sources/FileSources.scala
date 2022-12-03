package sparkStreaming.sources

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.{SparkIOUtil => SU}

object FileSources {

  def getSchema: StructType = {
    val schema = StructType(
      List(
        StructField("RecordNumber", IntegerType, true),
        StructField("Zipcode", StringType, true),
        StructField("ZipCodeType", StringType, true),
        StructField("City", StringType, true),
        StructField("State", StringType, true),
        StructField("LocationType", StringType, true),
        StructField("Lat", StringType, true),
        StructField("Long", StringType, true),
        StructField("Xaxis", StringType, true),
        StructField("Yaxis", StringType, true),
        StructField("Zaxis", StringType, true),
        StructField("WorldRegion", StringType, true),
        StructField("Country", StringType, true),
        StructField("LocationText", StringType, true),
        StructField("Location", StringType, true),
        StructField("Decommisioned", StringType, true)
      )
    )
    schema
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SU.getSparkSession
    spark.conf.set("spark.sql.shuffle.partition", 3)


    val schema = getSchema
    // Read from File Source
    val inputDF: DataFrame = spark.readStream
      .option("maxFilesPerTrigger", 1)
      .schema(getSchema)
      .json("./src/main/resources/folder_streaming/*.json")

    inputDF.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("30 second"))
      .start()
      .awaitTermination()

  }

}
