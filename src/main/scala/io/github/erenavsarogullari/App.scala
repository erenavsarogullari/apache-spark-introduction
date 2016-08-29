package io.github.erenavsarogullari

import org.apache.spark.sql.SparkSession

case class Person(name: String, age: Long)

/**
  * Created by erenavsarogullari on 8/29/16.
  */
object App {

  def main(args: Array[String]) {
    val spark = SparkSession
                  .builder()
                  .appName("apache-spark-introduction")
                  .master("local")
                  .getOrCreate()

    val distData = spark.sparkContext.parallelize(List(Person("a", 20), Person("b", 21), Person("c", 22)))

    import spark.implicits._

    val ds = distData.toDF().as[Person]
    ds.show()

  }

}
