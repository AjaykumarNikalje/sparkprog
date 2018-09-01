package com.scalaprog
import org.apache.spark._
import org.apache.spark.SparkContext._


/* /bin/spark-submit -class com.scalaprog.WordCount --master local[3] /home/nikaljeajay369029/jars/SparkApp.jar */
object WordCount {
  def main(args: Array[String]) {
  val conf = new SparkConf().setAppName("wordCount")
  // Create a Scala Spark Context.
  val sc = new SparkContext(conf)
  var text = sc.textFile("tenants/testing/wordcount.txt")
  var words = text.flatMap(line => line.split(","))
  // Transform into word and count.
  var counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
  counts.saveAsTextFile("tenants/testing/wordcountresult.txt")
  }
}
