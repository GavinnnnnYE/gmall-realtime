package com.gavin.gmallrealtime.canal

import java.net.InetSocketAddress
import java.util

import com.alibaba.otter.canal.client.{CanalConnector, CanalConnectors}
import com.alibaba.otter.canal.protocol.CanalEntry.{EntryType, EventType, RowChange}
import com.alibaba.otter.canal.protocol.{CanalEntry, Message}
import com.google.protobuf.ByteString

import scala.collection.JavaConverters._

object CanalClient {

  def main(args: Array[String]): Unit = {
    //1. 连接canal
    //  (SocketAddress address, String destination, String username,String password)
    val address = new InetSocketAddress("hadoop102",11111)
    val connector: CanalConnector = CanalConnectors.newSingleConnector(address, "example", "", "")
    connector.connect() //手动连接

    //2. 拉取数据
      //2.1 订阅想拉取的数据(表)
    connector.subscribe("gmall_realtime.*")
      //2.2 不断拉取数据
    while(true){
      // 100 表示最多拉取由于100条sql导致的变化的数据
      val message: Message = connector.get(100)
      //Entry => 一条sql导致的变化， 一个entry里有一个StoreValue存储变化的所有数据
      // 一个 StoreValue里有一个RowChange 包含多行数据的变化，RowChange里有多个RowData(包含一行数据)
      //RowData 里有列名和列值
      val entries: util.List[CanalEntry.Entry] = message.getEntries

      if(entries != null && !entries.isEmpty){
        for (entry <- entries.asScala){
          // entry 的类型必须是 ROWDATA
          if(entry != null && entry.hasEntryType && entry.getEntryType == EntryType.ROWDATA){
            val value: ByteString = entry.getStoreValue
            val rowChange: RowChange = RowChange.parseFrom(value)
            val rowDatasList: util.List[CanalEntry.RowData] = rowChange.getRowDatasList
            //3. 解析数据
            CanalHandler.handleData(rowDatasList,entry.getHeader.getTableName, rowChange.getEventType)
          }
        }
      }else{
        println("没有拉到数据，三秒后继续拉取。")
        Thread.sleep(3000)
      }
    }
  }
}
