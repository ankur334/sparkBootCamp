package utils

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

import scala.collection.mutable.{Map => MutableMap}

object RelationalDbUtil {

  def getData(spark: SparkSession, configs: Map[String, String], readType: String = "fullRead"): DataFrame = {
    
    val options = setJdbcOptions(configs)
    val extractedDF: DataFrame = readType match {
      case "fullRead" => fullRead(spark, options)
      case "incrementalRead" => incrementalRead(spark, options)
      case _ => throw new Exception(s"For now we have only implemented the ")
    }
    extractedDF
  }

  /***
   * Utils method to get the data from JDBC sources
   * We are setting following variables.
   *  query            The sql query to import the data.
   *                        The format of the query should be acceptable by dbtable option for Spark Jdbc
   *  url             JDBC url for the source data base
   *  user            User name to access the source data
   *  password        Password to access the source data.
   *  driver          the driver class name of the source jdbc driver
   *  numPartitions   Optional number of concurrent connections to be made for spark jdbc connection
   *  partitionColumn optional key column on which the data import is partitioned
   *  lowerBound      lower bound value of the key column
   *  upperBound      upper bound value of the key column
   *  fetchSize       optional size of the import jdbc fetch size
   */
  def setJdbcOptions(configs: Map[String, String]): MutableMap[String, String] = {
    val jdbcOptions = collection.mutable.Map(
      "url" -> configs.getOrElse("url", throw new Exception(s"Please pass the `url` as key")),
      "user" -> configs.getOrElse("user", throw new Exception(s"Please pass the `user` as key")),
      "password" -> configs.getOrElse("password", throw new Exception(s"Please pass the `password` as key"))
    )

    // Check for `query` and `dbtable` keys
    if(configs.contains("query") && configs.contains("dbtable")){
      throw new IllegalArgumentException(s"We can either have `query` or `dbtable`. We can't have both")
    } else if(configs.contains("query")){
      jdbcOptions += ("query" -> configs("query"))
    } else if(configs.contains("dbtable")){
      jdbcOptions += ("dbtable" -> configs("dbtable"))
    } else{
      throw new Exception(s"We either need `query` and `dbtable` as keys in Configs. You have passed None.")
    }

    // Check for `driver` key
    if(configs.contains("driver")){
      jdbcOptions += ("driver" -> configs("driver"))
    }

    // Check for `Number of partition` Key
    if(configs.contains("numPartitions")){
      if(configs.contains("partitionColumn") && configs.contains("lowerBound") && configs.contains("upperBound")){
        println("Here")
        jdbcOptions += (
          "numPartitions" -> configs("numPartitions"),
          "partitionColumn" -> configs("partitionColumn"),
          "lowerBound" -> configs("lowerBound"),
          "upperBound" -> configs("upperBound")
        )
      }else{
        val errorMessage = s"""
                       Error while reading from JDBC data sources.
                       Users need to specify all or none for the following
                       options: 'partitionColumn', 'lowerBound', 'upperBound', and 'numPartitions'"""
        throw new IllegalArgumentException(errorMessage)
      }
    }
    jdbcOptions
  }

  def fullRead(spark: SparkSession, configs: MutableMap[String, String]): DataFrame = {
    val extractedDF = jdbcFetch(spark, configs)
    extractedDF
  }

  def incrementalRead(spark: SparkSession, configs: MutableMap[String, String]): DataFrame = {
    spark.emptyDataFrame
  }

  def jdbcFetch(spark: SparkSession, options: MutableMap[String, String]): DataFrame = {

    try {
      val extractedDF: DataFrame  = spark.read.format(
        "jdbc"
      ).options(options).load()

      extractedDF
    }
    catch {
      case e: Exception => throw new UnsupportedOperationException("Error JDBC Fetch - " + e)
    }

  }


  def upsertData(oldDf: DataFrame, newDf: DataFrame): DataFrame = {
    val combinedDF = oldDf.union(newDf)
    combinedDF.show(truncate = false)
    combinedDF.printSchema()

    val windowSpec = Window.partitionBy("seller_id").orderBy(col("creation_date").desc)

    val rankedDF = combinedDF.withColumn(
      "r_number",
      row_number().over(windowSpec)
    )

    rankedDF.show()
    rankedDF.printSchema()

    val filteredDF = rankedDF.filter(col("r_number") === 1).drop("r_number")

    filteredDF.show()
    filteredDF.printSchema()

    filteredDF

  }
  
}
