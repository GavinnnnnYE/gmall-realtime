package com.gavin.realtime.app
import com.alibaba.fastjson.JSON
import com.gavin.gmallrealtime.constant.GmallConstant
import com.gavin.realtime.bean.OrderInfo
import com.gavin.realtime.util.MyKafkaUtil
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

object OrderApp extends BaseApp {
  override def run(ssc: StreamingContext): Unit = {
    //拿到流
    val sourceStream: DStream[String] = MyKafkaUtil.getKafkaStream(ssc, GmallConstant.TOPIC_ORDER_INFO)
    val orderInfoStream: DStream[OrderInfo] = sourceStream.map(json => {
      JSON.parseObject(json, classOf[OrderInfo])
    })

    //把数据写入到HBase
    orderInfoStream.foreachRDD(rdd => {
      import org.apache.phoenix.spark._
      rdd.saveToPhoenix("GMALL_ORDER_INFO",
        Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS",
          "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS",
          "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY",
          "CREATE_DATE", "CREATE_HOUR"),
        zkUrl = Option("hadoop102,hadoop103,hadoop104:2181")
        )
    })
    orderInfoStream.print(100)
  }
}
