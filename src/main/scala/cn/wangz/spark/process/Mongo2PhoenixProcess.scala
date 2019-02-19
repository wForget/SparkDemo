package cn.wangz.spark.process

import com.mongodb.spark.MongoSpark
import org.apache.spark.SparkContext
import com.mongodb.spark.config.ReadConfig
import org.apache.commons.lang3.StringUtils
import org.apache.phoenix.spark._
import org.apache.spark.sql.SparkSession

/**
  * Created by hadoop on 2019/1/14.
  */
class Mongo2PhoenixProcess (private var sparkContext: SparkContext) {

//  val sparkSession = SparkSession.builder().config(sparkContext.getConf).getOrCreate()

  def process(): Unit = {

    val uri = "mongodb://username:password&F@servicenode06,servicenode07,servicenode08/admin?authMechanism=SCRAM-SHA-1&replicaSet=replSet01";

    var matchIndexRDD = MongoSpark.load(sparkContext, ReadConfig(Map("uri" -> uri
      , "database" -> "pubg"
      , "collection" -> "match_index")))
      .filter(m => (StringUtils.isNotBlank(m.getString("lowerNickName")) && m.getDate("startedAt") != null))
      .map(m => (m.getString("lowerNickName"), m.getDate("startedAt"), m.getString("matchId")
        , m.getString("mode"), m.getInteger("queueSize"), m.getInteger("totalRank"), m.getString("type")))

    matchIndexRDD.saveToPhoenix(
      "PUBG.MATCH_INDEX",
      Seq("LOWERNICKNAME", "STARTEDAT", "MATCHID", "MODE", "QUEUESIZE", "TOTALRANK", "TYPE"),
      zkUrl = Some("datanode03:2181")
    )
  }
}

//case class MatchIndex(lowerNickName: String,
//                      startedAt: java.sql.Date,
//                      matchId: String,
//                      mode: String,
//                      queueSize: Int,
//                      totalRank: Int,
//                      `type`: String)