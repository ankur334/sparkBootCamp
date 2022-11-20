package sparkStreaming.structuredStreaming

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, explode, lit, split}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.{SparkIOUtil => SU}


object wordCount {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SU.getSparkSession
    spark.sparkContext.setLogLevel("ERROR")

    spark.conf.set("spark.sql.shuffle.partitions", 3)
    println(s"Spark Conf ${spark.conf.getAll}")


    val sc: SparkContext = spark.sparkContext

    val ssc = new StreamingContext(
      sc,
      batchDuration = Seconds(10)
    )

//    ssc.checkpoint("./checkpointing")

    // Read from Socket
    val linesDF: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9990")
      .load()

    val wordsDF: DataFrame = linesDF.withColumn(
      s"words",
      explode(split(col("value"), " "))
    ).withColumn("frequency", lit(1))
      .select("words", "frequency")

    val resultDF = wordsDF.groupBy("words").agg(
      functions.sum("frequency").as("wordsFrequency")
    )

    // Show in Console

    resultDF.writeStream
      .format("console")
      .outputMode("complete")
      .option("checkpointLocation", "./checkpointing")
      .trigger(Trigger.ProcessingTime("30 Seconds"))
      .start()
      .awaitTermination()



  }

}
