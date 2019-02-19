package cn.wangz.spark.process

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

/**
  * Created by hadoop on 2019/2/19.
  * spark 读取 hbase 两种方式
  */
class ReadHbaseProcess (private var sparkContext: SparkContext) {

  def process(): Unit = {
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "datanode03,datanode05,datanode04")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")

    val scan = new Scan()
    scan.setTimeRange(0L, 1550546631000L)
    val hBaseContext = new HBaseContext(sparkContext, hbaseConf)
    val matchRdd = hBaseContext.hbaseRDD(TableName.valueOf("match_detail"), scan).map(r => {
      val rowkey = Bytes.toString(r._1.copyBytes())
      val value = Bytes.toString(r._2.getValue("v".getBytes(), "value".getBytes()))
      if (StringUtils.isBlank(value)) {
        null
      } else {
        // process value GsonUtil.fromJson(value, classOf[Match])
      }
    })
      .filter(m => m != null)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    //    val matchRdd = sparkContext.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat]
    //      , classOf[ImmutableBytesWritable], classOf[Result])
    //      .map(r => {
    //        val rowkey = Bytes.toString(r._1.copyBytes())
    //        val value = Bytes.toString(r._2.getValue("v".getBytes(), "value".getBytes()))
    //        if (StringUtils.isBlank(value)) {
    //          null
    //        } else {
    //          GsonUtil.fromJson(value, classOf[Match])
    //        }
    //      })
    //      .filter(m => m != null)
    //      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    // TODO something
  }

}
