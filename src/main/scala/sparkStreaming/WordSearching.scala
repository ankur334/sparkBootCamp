package sparkStreaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import sparkStreaming.WordCountStreaming.{BATCH_INTERVAL_TIME, updateFunc}
import utils.{SparkIOUtil => SU}

object WordSearching {

  def stateless(ssc: StreamingContext, wordsFrequencyMap: DStream[(String, Int)]): Unit = {

    val filterMap: DStream[(String, Int)] = wordsFrequencyMap.filter(x=> {
      x._1.startsWith("big")
    })

    val result = filterMap.reduceByKey(_ + _)

    result.print()

  }

  def stateFullEntireStream(wordsFrequencyMap: DStream[(String, Int)]): Unit = {
    val filterMap: DStream[(String, Int)] = wordsFrequencyMap.filter(x=> {
      x._1.startsWith("big")
    })

    val result = filterMap.updateStateByKey(updateFunc)

    result.print()
  }

  def stateFullSlidingWindow(wordsFrequencyMap: DStream[(String, Int)],
                             windowIntervalTime: Int, slidingIntervalTime: Int): Unit = {

    def summaryFunc(x: Int, y:Int): Int = {x + y}

    def inverseFunc(x: Int, y:Int): Int = {x - y}

    val filterMap: DStream[(String, Int)] = wordsFrequencyMap.filter(x=> {
      x._1.startsWith("big")
    })

    val result = filterMap.reduceByKeyAndWindow(
      summaryFunc(_, _), inverseFunc(_, _),
      Seconds(windowIntervalTime), Seconds(slidingIntervalTime)
    )

    result.print()


  }

  def main(args: Array[String]): Unit = {
    val batch_duration_time = args.head

    val spark: SparkSession = SU.getSparkSession
    spark.sparkContext.setLogLevel("ERROR")
    val sc: SparkContext = spark.sparkContext
    val ssc: StreamingContext = new StreamingContext(
      sc,
      batchDuration = Seconds(2)
    )

    ssc.checkpoint("./checkpointing")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9998)

    val words: DStream[String] = lines.flatMap(w => w.split(" "))

    val wordsFrequencyMap: DStream[(String, Int)] = words.map(w => (w, 1))

//    stateless(ssc, wordsFrequencyMap)

    stateFullSlidingWindow(wordsFrequencyMap, 10, 4)



    ssc.start()
    ssc.awaitTermination()


  }
}
