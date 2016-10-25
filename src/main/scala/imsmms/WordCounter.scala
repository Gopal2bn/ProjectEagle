package imsmms

import org.apache.spark.rdd.RDD

/**
  * Created by gopal on 10/16/16.
  */
object WordCounter {
 def count(inputRDD: RDD[String]): RDD[(String, Int)] = {
  val countByWordRDD = inputRDD.flatMap(_.split("\\W+"))
    .map(w => (w, 1))
    .reduceByKey(_ + _)
  countByWordRDD
 }
}
