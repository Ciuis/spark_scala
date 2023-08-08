package ru.ciuis.spark_scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

import java.util.Properties

object MainFoodPricesRus {
  def main(args: Array[String]): Unit = {
    val app = MainFoodPricesRus
    app.start()
  }

  def start(): Unit = {
    // change this to yours data source
    val src = "./datasets/wfp_food_prices_rus.csv"

    val spark = SparkSession
      .builder
      .appName("food prices ")
      .master("local[*]")
      .getOrCreate()

    val pricesRus = spark.read
      .format("csv")
      .option("header", "true")
      .load(src)

    val window = Window.partitionBy("commodity").orderBy(col("date"))

    val newPrices = pricesRus
      .filter(col("date") =!= "#date")
      .withColumn("price_diff", round(col("price") - lag(col("price"), 1).over(window),3))
      .withColumn("usdprice_diff", round(col("usdprice") - lag(col("usdprice"), 1).over(window), 3))

    val dbConnectionUrl: String = "jdbc:postgresql://localhost/postgres"
    val props = new Properties()
    props.setProperty("driver", "org.postgresql.Driver")
    props.setProperty("user", "postgres")
    props.setProperty("password", "123456")

    newPrices.write.mode("overwrite")
      .jdbc(dbConnectionUrl, "food_market.prices_diff", props)

    println("***** DATA WRITTEN TO DB *****")

    spark.stop()
  }
}
