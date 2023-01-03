package org.example

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkListenerDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .master("local[*]")
        .appName("Spark Listener Demo")
        .getOrCreate()

    //通过 addSparkListener 注册
    //也可以通过config注册：spark-submit --conf "spark.extraListeners=org.example.MySparkListener"
    spark.sparkContext.addSparkListener(new MySparkListener)

    //通过 listenerManager 注册
    //可以通过config注册：spark-submit --conf "spark.extraListeners=org.example.MyQueryExecutionListener"
    spark.listenerManager.register(new MyQueryExecutionListener)

    try {
      println("\n------------ load city.json")
      val schema = new StructType()
          .add("no", IntegerType)
          .add("cityName", StringType)
          .add("province", StringType)
          .add("code", IntegerType)
      val df2 = spark.read.schema(schema).json("city.json")

      println("------------ write table city")
      df2.write.mode(SaveMode.Overwrite)
          .option("path", "./spark-warehouse/city")
          .saveAsTable("city")

      println("\n------------ SELECT * FROM city")
      spark.sql("SELECT * FROM city").show
    } catch {
      case e: IllegalArgumentException =>
        e.printStackTrace()
      case e: Exception =>
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}



