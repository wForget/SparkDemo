package cn.wangz.spark.job

import cn.wangz.spark.process.Mongo2PhoenixProcess
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hadoop on 2019/1/14.
  */
object Mongo2PhoenixJob {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
//      .setMaster("local[1]")
      .setAppName(s"Mongo2PhoenixJob")
    val sc = new SparkContext(sparkConf)
    val process = new Mongo2PhoenixProcess(sc)
    process.process()
  }
}
