package ru.ciuis.spark_scala

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, streaming}

import java.util.Properties

object CollegeMain {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("read_csv")
      .master("local")
      .getOrCreate()

    val source = "./datasets/College_Data.csv"
    var df: DataFrame = spark.read.format("csv")
      .option("header", "true")
      .load(source)

    df = df.withColumn(
      "perc_accept",
      df.col("Accept") / df.col("Apps")
    )
    df = df.withColumn(
      "perc_enroll",
      df.col("Enroll") / df.col("Accept")
    )

    val dbConnectionUrl: String = "jdbc:postgresql://localhost/postgres"
    val prop: Properties = new Properties()
    prop.setProperty("driver", "org.postgresql.Driver")
    prop.setProperty("user", "postgres")
    prop.setProperty("password", "123456")

    df.write.mode("append")
      .mode(SaveMode.Overwrite)
      .jdbc(dbConnectionUrl, "spark_test", prop)
    //df.show(false)
    println("DATA WRITTEN TO DB")
  }
}
