package sparkStreaming.windowsStatefull

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SparkSession, functions}

import org.apache.spark.sql.functions.{col, from_json, window}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.{SparkIOUtil => SU}


object windowFunction {

  def getSchema: StructType = {
    val schema = StructType(
      List(
        StructField("order_id", IntegerType),
        StructField("order_date", TimestampType),
        StructField("order_customer_id", StringType),
        StructField("order_status", StringType),
        StructField("amount", IntegerType),
      )
    )
    schema
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SU.getSparkSession
    spark.sparkContext.setLogLevel("ERROR")

    spark.conf.set("spark.sql.shuffle.partitions", 3)


    val inputDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9998)
      .load()

    val rawDF = inputDF.select(
      from_json(col("value"), getSchema).as("value")
    ).select("value.*")

    rawDF.printSchema()

    val windowsAggregateDF = rawDF.groupBy(
      window(col("order_date"), "15 minute")
    ).agg(
      functions.sum("amount").alias("total_sale")
    ).select("window.*", "total_sale")
    windowsAggregateDF.printSchema()

    val writeDF = windowsAggregateDF.writeStream
      .format("console")
      .outputMode("complete")
      .option("checkpointLocation", "./checkpointing2")
      .trigger(Trigger.ProcessingTime("30 Seconds"))
      .start()


    writeDF.awaitTermination()

  }



}
