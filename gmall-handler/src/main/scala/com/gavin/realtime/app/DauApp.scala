package com.gavin.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.gavin.gmallrealtime.constant.GmallConstant
import com.gavin.realtime.bean.StartupLog
import com.gavin.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object DauApp {
  def main(args: Array[String]): Unit = {
    // 1.创建一个 StreamingContext
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("DauApp")
    val ssc = new StreamingContext(conf, Seconds(3))

    // 2. 获取一个流
    val sourceStream: DStream[String] = MyKafkaUtil.getKafkaStream(ssc, GmallConstant.TOPIC_STARTUP)

    // 2.1 把每个json字符串的数据,封装到一个样例类对象中
    val startupLogStream: DStream[StartupLog] = sourceStream.map(json => JSON.parseObject(json, classOf[StartupLog]))

    //【 Driver(1) 读Redis不能在这，不然他从头到尾就读这么一次! 】

    // 3. 去重 (most important) - 过滤掉已经启动过的设备的记录  - 从redis里读已经启动过的设备id
    val filteredStartupLogStream: DStream[StartupLog] = startupLogStream.transform(rdd => {

      //【 transform => 每一批次调度一次,连redis的操作放这里最合适! (因为filter没有filterPartition算子)】
      //transform 能把流的操作转换成对rdd的操作，因为流里边提供的算子不够丰富，而且适合做一些对外界的操作

      // 3.1 先去读redis的数据   mid都存在一个 set集合中, "mids:" + ...
      // Code Driver (N)
      val client: Jedis = RedisUtil.getClient
      val mids: util.Set[String] = client.smembers(GmallConstant.TOPIC_STARTUP + ":"
        + new SimpleDateFormat("yy-MM-dd").format(new Date()))

      client.close()     //这一步不能忘，不然一会池子就用完了！

      // 3.2 必须把集合做一个广播变量，避免一个Executor存多份(每个分区/Task存1份)变量，造成性能浪费
      val midBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(mids)
      // 返回那些没有启动过的设备的启动记录

      rdd.filter(
        //Executor(N) 读 Redis 也不能在这读，不然来一条数据就连一次 redis, 这不是分分钟爆炸吗?!
        startupLog => !midBC.value.contains(startupLog.mid) //广播变量要通过.value调取
      )
        //同批次内还要去一次重(同一个之前没登陆过的设备在3s内多次登陆，是无法通过黑名单过滤掉的)，不然多条数据会进入 HBase 中
        .map(log => (log.mid, log))
        .groupByKey()
        .map {
          case (_, logs) => logs.toList.minBy(_.ts)
        }
    })
    // 3.3 把第一次启动的记录写入到 redis 中
    filteredStartupLogStream.foreachRDD(rdd => {
      // rdd的数据写入到redis, 只需要写 mid 就行了
      // 一个分区一个分区的写
      rdd.foreachPartition(logs => {
        val client: Jedis = RedisUtil.getClient
        logs.foreach(log => {
          client.sadd(GmallConstant.TOPIC_STARTUP + ":" + log.logDate, log.mid) //key要体现出日期，因为黑名单是根据当天来的
        })
        client.close()  // 又忘记关这个了，要记得啊！
      })
      // 4. 数据写入到 HBase中 (当天启动的设备的第一条启动记录)
      import org.apache.phoenix.spark._ //rdd 本身不自带saveToPhoenix() 这个放法，必须导一下这个
      rdd.saveToPhoenix(
        "GMALL_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CHANNEL", "LOGTYPE", "VERSION", "TS", "LOGDATE", "LOGHOUR"),
        zkUrl = Some("hadoop102,hadoop103,hadoop104:2181")
      )
    })

    // 5. 对流做输出 (output)
    filteredStartupLogStream.print(10000)

    // 6. 开启流
    ssc.start()

    // 7. 阻止main退出
    ssc.awaitTermination()
  }
}
