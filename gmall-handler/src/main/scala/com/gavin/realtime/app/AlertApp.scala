package com.gavin.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.gavin.gmallrealtime.constant.GmallConstant
import com.gavin.realtime.bean.{AlertInfo, EventLog}
import com.gavin.realtime.util.MyKafkaUtil
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

import scala.util.control.Breaks._

object AlertApp extends BaseApp {
  override def run(ssc: StreamingContext): Unit = {
    val enventLogStream: DStream[String] =
      MyKafkaUtil.getKafkaStream(ssc, GmallConstant.TOPIC_EVENT)
    val eventLogStream: DStream[EventLog] = enventLogStream.map(json => JSON.parseObject(json, classOf[EventLog]))
      .window(Minutes(5), Seconds(6))

    // 1, 按照设备id进行分组
    val groupedEventStream: DStream[(String, Iterable[EventLog])] = eventLogStream.map(log => (log.mid, log))
      .groupByKey()

    // 2. 产生预警信息
    val alertInfoStream: DStream[(Boolean, AlertInfo)] = groupedEventStream.map {
      // eventLogIt 表示当前mid上5分钟内所有的事件
      case (mid, eventLogIter) =>
        // 领取优惠券的用户
        val uidSet: util.HashSet[String] = new util.HashSet[String]()
        // 存储优惠券对应的那些商品id
        val itemSet = new util.HashSet[String]()
        // 存储优惠券对应的那些商品id
        val eventList = new util.ArrayList[String]()
        // 是否点击过商品
        var isClickItem = false

        breakable {
          eventLogIter.foreach(log => {
            // 把事件id添加到eventList
            eventList.add(log.eventId)
            // 只关注领取优惠券的用户
            log.eventId match {
              case "coupon" =>
                uidSet.add(log.uid)
                itemSet.add(log.itemId)

              case "clickItem" =>
                isClickItem = true
                break

              case _ => //其他事件不做任何处理
            }
          })
        }
        // 返回 => (是否预警，AlertInfo(...))
        (!isClickItem && uidSet.size() >= 3, AlertInfo(mid, uidSet, itemSet, eventList, ts = System.currentTimeMillis()))
    }
    alertInfoStream.print(1000)
  }
}

/*
----
需求：同一设备，5分钟内三次及以上用不同账号登录并领取优惠劵，
并且在登录到领劵过程中没有浏览商品。同时达到以上要求则产生一条预警日志。
 同一设备，每分钟只记录一次预警。

同一设备  ->   group by mid_id
5分钟内的数据, 每6秒统计一次 -> 窗口 窗口的长度: 5分钟  窗口的步长:6s

三次及以上用不同账号登录  -> 统计每个设备的登录的用户数
领取优惠劵  -> 统计领取优惠券的行为

并且在登录到领劵过程中没有浏览商品 -> 事件中没有浏览商品行为

----

同一设备，每分钟只记录一次预警。  -> 不在spark-streaming 完成, 让es来完成


// 1. reduceByKeyAndWindow
   2. 直接在流上使用window, 将来所有的操作都是基于这个窗口

*/