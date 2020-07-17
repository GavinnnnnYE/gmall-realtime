package com.gavin.realtime.util

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object MyKafkaUtil {
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> ConfigUtil.getProperty("config.properties","kafka.servers"),
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> ConfigUtil.getProperty("config.properties","group.id"),
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )

  def getKafkaStream(ssc: StreamingContext, topic: String) = {
    KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent, //标配，大多数情况都用这个
      Subscribe[String, String](Set(topic), kafkaParams)  //订阅主题
    ).map(_.value()) //用不到key，只要value
  }


}
