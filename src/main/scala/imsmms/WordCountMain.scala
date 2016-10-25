package imsmms

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by gopal on 10/16/16.
  */
object WordCountMain {
  val conf= new SparkConf().setAppName("Simple Word Count Main")
  val sc=new SparkContext(conf)

  def main(args:Array[String]){
    val inputPath=args(0)
    val outputPath=args(1)
    val inputRDD= sc.textFile(inputPath)
    val countByWordRDD: RDD[(String, Int)] = WordCounter.count(inputRDD)
    countByWordRDD.saveAsTextFile(outputPath)
  }
}
