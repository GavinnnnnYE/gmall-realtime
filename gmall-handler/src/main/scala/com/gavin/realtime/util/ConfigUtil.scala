package com.gavin.realtime.util

import java.io.InputStream
import java.util.Properties

object ConfigUtil {
  def getProperty(fileNmae: String, name: String) ={
    val ins: InputStream = ConfigUtil.getClass.getClassLoader.getResourceAsStream(fileNmae)
    val properties = new Properties()
    properties.load(ins)
    properties.getProperty(name)
  }

//   //TEST
//  def main(args: Array[String]): Unit = {
//    println(getProperty("config.properties","group.id"))
//  }

}
