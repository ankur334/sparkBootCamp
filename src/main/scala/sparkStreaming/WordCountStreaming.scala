package sparkStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import utils.{SparkIOUtil => SU}

object WordCountStreaming {

  Logger.getLogger("org").setLevel(Level.ERROR)

  private val BATCH_INTERVAL_TIME = 2
  private val WINDOWS_INTERVAL_TIME = 10 // Windows size should also be integral multiple of batch interval
  private val SLIDING_INTERVAL_TIME = 2 // Sliding interval should always be integral multiple of batch interval

  def countWordsStateLess(lines: ReceiverInputDStream[String]): Unit = {
    val words: DStream[String] = lines.flatMap(x => x.split(" "))

    val wordsFrequencyMap: DStream[(String, Int)] = words.map(w => {
      (w, 1)
    })

    val count: DStream[(String, Int)] = wordsFrequencyMap.reduceByKey(_ + _)

    count.print()
  }

  def countWordsStaleFull(lines: ReceiverInputDStream[String], readType: String,
                          windowsTime: Int = 0, slidingIntervalTime: Int = 0): Unit = {
    if (readType == "entireStream") {
      countWordsStateFullEntireStream(lines)
    } else {
      countWordsStateFullWithSlidingWindows(lines, windowsTime, slidingIntervalTime)
    }

  }

  protected def countWordsStateFullEntireStream(lines: ReceiverInputDStream[String]): Unit = {
    val words: DStream[String] = lines.flatMap(w => w.split(" "))

    val wordsFrequencyMap: DStream[(String, Int)] = words.map(w => (w, 1))

    val wordsCount: DStream[(String, Int)] = wordsFrequencyMap.updateStateByKey(updateFunc = updateFunc)

    wordsCount.print()
  }


  protected def countWordsStateFullWithSlidingWindows(lines: ReceiverInputDStream[String],
                                                      windowsTime: Int, slidingIntervalTime: Int): Unit = {

    val words: DStream[String] = lines.flatMap(x => x.split(" "))

    val wordsFrequencyMap: DStream[(String, Int)] = words.map(w => {
      (w, 1)
    })

    val count: DStream[(String, Int)] = wordsFrequencyMap.reduceByKeyAndWindow(
      summaryFunc(_,_), inverseFunc(_,_), Seconds(windowsTime), Seconds(slidingIntervalTime)
    )

    count.print()

  }

  def updateFunc(newValue: Seq[Int], previousState: Option[Int]): Option[Int] = {
    val newCount: Int = previousState.getOrElse(0) + newValue.sum

    Some(newCount)
  }

  def summaryFunc(a: Int, b: Int): Int = {
    a + b
  }

  def inverseFunc(a: Int, b: Int): Int = {
    a - b
  }

  def main(): Unit = {



    val spark: SparkSession = SU.getSparkSession
    spark.sparkContext.setLogLevel("ERROR")

    val sc: SparkContext = spark.sparkContext

    val ssc = new StreamingContext(
      sc,
      batchDuration = Seconds(BATCH_INTERVAL_TIME)
    )

    ssc.checkpoint("./checkpointing")

    // Here In our code base batch interval is 2 seconds means after every 2 seconds. New RDD will be created.
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9998)


    // countWordsStateLess(lines)

    // countWordsStaleFull(lines, "entireStream")

    // Here In our code base batch interval is 2 seconds means after every 2 seconds. New RDD will be created.
    // We are setting windows interval time of 10 second it means that we are only interested in last 5 RDD.
    // Because 10 / 2 = 5 RDDs
    // & As we have set up sliding interval of 2 second then after each 2 seconds. One RDD will removed and one added.
    countWordsStaleFull(
      lines,
      readType = "windowsStream",
      windowsTime = WINDOWS_INTERVAL_TIME,
      slidingIntervalTime = SLIDING_INTERVAL_TIME
    )


    ssc.start()
    ssc.awaitTermination()

  }

}
