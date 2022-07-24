package utils

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.{Map => MutableMap}

object RelationDbUtilDemo {

  def getData(spark: SparkSession, configs: Map[String, String], readType: String = "fullLoad"): DataFrame = {

    val jdbcOptions = setJdbcOption(configs)
    val extractedDF = readType match {
      case "fullLoad" => getFullData(spark, jdbcOptions)
      case "incrementalLoad" => getIncrementalData(spark, jdbcOptions)
      case _ => throw new Exception(s"For now we only support `fullLoad` & `incrementalLoad`.")
    }

    extractedDF
  }

  def getFullData(spark: SparkSession, options: MutableMap[String, String]): DataFrame = {

    try {
      spark.read.format(
        "jdbc"
      ).options(options).load()
    } catch {
      case e: Exception =>
        throw new Exception(e.getMessage)
    }
  }

  def getIncrementalData(spark: SparkSession, configs: MutableMap[String, String]): DataFrame = {
    // TODO::
    spark.emptyDataFrame
  }

  def setJdbcOption(configs: Map[String, String]): MutableMap[String, String] = {

    val jdbcOptions =  MutableMap(
      "url" -> configs.getOrElse("url", throw new IllegalArgumentException("Please pass `url` as key")),
      "user" -> configs.getOrElse("user", throw new Exception(s"Please pass the `user` as key")),
      "password" -> configs.getOrElse("password", throw new Exception(s"Please pass the `password` as key")),
      "port" -> configs.getOrElse("port", throw new Exception(s"Please pass the `port` as key"))
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

}
