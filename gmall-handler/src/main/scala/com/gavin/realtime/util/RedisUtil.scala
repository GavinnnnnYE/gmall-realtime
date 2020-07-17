package com.gavin.realtime.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

//搞一个连接池
object RedisUtil {
  val host: String = ConfigUtil.getProperty("config.properties","redis.host")
  val port: String = ConfigUtil.getProperty("config.properties","redis.port")
  private val config = new JedisPoolConfig
  config.setMaxTotal(100) //最大线程数
  config.setMaxIdle(30) //最大空闲
  config.setMinIdle(10) //最小空闲
  config.setBlockWhenExhausted(true) //忙碌时是否等待
  config.setMaxWaitMillis(5000) //忙碌时等待时长 毫秒
  config.setTestOnCreate(true)  //测试创建连接
  config.setTestOnBorrow(true) //获得连接的测试
  config.setTestOnReturn(true)  //测试能否归还

  private val pool = new JedisPool(config, host, port.toInt)

  def getClient: Jedis = {
    pool.getResource
  }
}
