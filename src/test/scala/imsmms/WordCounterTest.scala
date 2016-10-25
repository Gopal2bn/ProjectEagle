package imsmms

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}


/**
  * Created by gopal on 10/16/16.
  */
class WordCounterTest extends FlatSpec with Matchers with BeforeAndAfter {

  var sc: SparkContext = _

  before {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("test-wordcount")
    sc = new SparkContext(sparkConf)
  }
  after {
    sc.stop()
  }

  behavior of "WordCounter"
  it should "count words in a text" in {
    val text=
      """Hello Spark
        |Hello world
        |Welcome to Spark
      """.stripMargin
    val lines : RDD[String] = sc.parallelize(List(text))
    val wordCounts: RDD[(String, Int)] = WordCounter.count(lines)
    wordCounts.collect() should contain allOf(("Spark",2),("Hello",2),("world",1),("to",1),("Welcome",1))
  }

}
