package ru.ciuis.spark_scala

import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import java.util.Properties

object sparkJSONMain {
  def main(args: Array[String]): Unit = {
    val app = sparkJSONMain
    app.start()
  }

  def start(): Unit = {
    val source: String = "./datasets/profiles.json"
    val spark: SparkSession = SparkSession.builder()
      .appName("sparkJSON")
      .master("local")
      .getOrCreate()

    val df: DataFrame = spark.read.format("json")
      .option("header", "true")
      .load(source)

    //df.show(false)
    //df.printSchema()
    val dbConnectionUrl: String = "jdbc:postgresql://localhost/postgres"
    val prop: Properties = new Properties()
    prop.setProperty("driver", "org.postgresql.Driver")
    prop.setProperty("user", "postgres")
    prop.setProperty("password", "123456")

    df.write.mode("append")
      .mode(SaveMode.Overwrite)
      .jdbc(dbConnectionUrl, "nfl_players", prop)
    println("DATA WRITTEN TO DB")
  }
}
