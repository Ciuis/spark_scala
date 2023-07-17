package ru.ciuis.spark_scala

import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.lit
import org.apache.spark.Partition
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import java.util.Properties

object RestaurantsMain {
  def main(args: Array[String]): Unit = {
    val app = RestaurantsMain
    app.start()
  }

  private def start(): Unit = {
    val source: String = "./datasets/Restaurants_in_Wake_County.csv"

    lazy val spark: SparkSession = SparkSession.builder
      .appName("Restaurants in Wake County, NC")
      .master("local")
      .getOrCreate()

    var df: DataFrame = spark.read.format("csv")
      .option("header", "true")
      .load(source)

    df = df
      .withColumn("county", lit("Wake"))
      .withColumnRenamed("HSISID", "datasetId")
      .withColumnRenamed("Name", "name")
      .withColumnRenamed("ADDRESS1", "address1")
      .withColumnRenamed("ADDRESS2", "address2")
      .withColumnRenamed("CITY", "city")
      .withColumnRenamed("STATE", "state")
      .withColumnRenamed("POSTALCODE", "zip")
      .withColumnRenamed("PHONENUMBER", "tel")
      .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
      .withColumnRenamed("FACILITYTYPE", "type")
      .withColumnRenamed("X", "geoX")
      .withColumnRenamed("Y", "geoY")
      .drop("OBJECTID")
      .drop("PERMITID")
      .drop("GEOCODESTATUS")
    df = df
      .withColumn("id", concat(
        df.col("state"), lit("_"),
        df.col("county"), lit("_"),
        df.col("datasetId")
      ))

    println("DATASET TRANSFORM SUCCESSFUL")
    df.printSchema()
    println(s"Dataset has ${df.count()} records")

    println("*** Looking at partitions")
    val partitions: Array[Partition] = df.rdd.partitions
    val partitionCount: Int = partitions.length
    println(s"Partition count before repartition $partitionCount")

    println("*** Repartitioning")
    df = df.repartition(4)
    println(s"Partition count after repartition ${df.rdd.partitions.length}")

    df.show(false)

    val dbConnectionUrl: String = "jdbc:postgresql://localhost/postgres"
    val prop: Properties = new Properties()
    prop.setProperty("driver", "org.postgresql.Driver")
    prop.setProperty("user", "postgres")
    prop.setProperty("password", "123456")
    df.write.mode("append")
      .mode(SaveMode.Overwrite)
      .jdbc(dbConnectionUrl, "restaurants", prop)
    println("DATA WRITTEN TO DB")

    val schema: StructType = df.schema
    val schemaAsJSON = schema.prettyJson
    println(s"*** Schema as JSON $schemaAsJSON")

    spark.stop()
  }
}
