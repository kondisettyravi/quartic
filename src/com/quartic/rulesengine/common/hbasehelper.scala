package com.quartic.rulesengine.common

import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result}
import com.quartic.rulesengine.common.constants.{HBASE_CLIENT_RETRIES, HBASE_COL_NEXT, HBASE_STATUS_COL, VALUE_COL, VALUE_TYPE_COL, HBASE_COL_PARTITION, HBASE_COL_TOPIC, HBASE_DEFAULT_CF, HBASE_SIGNAL_COL}
import org.apache.hadoop.hbase.util.Bytes

object hbasehelper {

  def getHbaseConfiguration(): Configuration = {
    val hbaseConfig = HBaseConfiguration.create()
    hbaseConfig.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))
    hbaseConfig.addResource(new Path("/etc/hadoop/conf/core-site.xml"))
    hbaseConfig.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"))
    hbaseConfig.set("hbase.client.retries.number", HBASE_CLIENT_RETRIES)
    hbaseConfig
  }

  def createTableIfNotExists(name: String, config: Configuration): TableName = {
    val connection = ConnectionFactory.createConnection(config)
    val hAdmin = connection.getAdmin
    val tableName = TableName.valueOf(name)

    if (!hAdmin.tableExists(tableName)) {
      val tableDesc = new HTableDescriptor(tableName)
      tableDesc.addFamily(new HColumnDescriptor(HBASE_DEFAULT_CF))
      hAdmin.createTable(tableDesc)
      println("%s hbase table didnt exist; created".format(tableName))
      if (!hAdmin.isTableEnabled(tableName)) {
        println("%s hbase table was not enabled; enabling".format(tableName))
        hAdmin.enableTable(tableName)
      }
    }
    connection.close
    return tableName
  }

  def getValue(result: Result, cf: String, qual: String): String = {
    Bytes.toString(result.getValue(Bytes.toBytes(cf), Bytes.toBytes(qual)))
  }

  def getValueAsLong(result: Result, cf: String, qual: String): Long = {
    if (!result.containsColumn(Bytes.toBytes(cf), Bytes.toBytes(qual)))
      return 0
    val res = result.getValue(Bytes.toBytes(cf), Bytes.toBytes(qual))
    if (res == null)
      return 0
    Bytes.toLong(res)
  }

  def getValueAsInt(result: Result, cf: String, qual: String): Int = {
    return getValueAsInt(result, cf, qual, 0)
  }

  def getValueAsInt(result: Result, cf: String, qual: String, default: Int): Int = {
    if (!result.containsColumn(Bytes.toBytes(cf), Bytes.toBytes(qual)))
      return default
    val res = result.getValue(Bytes.toBytes(cf), Bytes.toBytes(qual))
    try {
      return Bytes.toInt(res)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        return default
    }
  }

  def commitOffset(tableName: String, partitionId: Integer, topic: String, untilOffset: Long) {
    val connection = ConnectionFactory.createConnection(hbasehelper.getHbaseConfiguration)
    val hCounter = connection.getTable(TableName.valueOf(tableName))
    val cPut = new Put(Bytes.toBytes(partitionId + "_" + topic))
    cPut.addColumn(Bytes.toBytes(HBASE_DEFAULT_CF), Bytes.toBytes(HBASE_COL_PARTITION), Bytes.toBytes(partitionId))
    cPut.addColumn(Bytes.toBytes(HBASE_DEFAULT_CF), Bytes.toBytes(HBASE_COL_TOPIC), Bytes.toBytes(topic))
    cPut.addColumn(Bytes.toBytes(HBASE_DEFAULT_CF), Bytes.toBytes(HBASE_COL_NEXT), Bytes.toBytes(untilOffset))
    hCounter.put(cPut)
    hCounter.close
    connection.close
  }
  
  def writeToHbase(input: Map[String, String], tableName: String) {
    val connection = ConnectionFactory.createConnection(hbasehelper.getHbaseConfiguration)
    val hCounter = connection.getTable(TableName.valueOf(tableName))
    val cPut = new Put(Bytes.toBytes(UUID.randomUUID().toString()))
    cPut.addColumn(Bytes.toBytes(HBASE_DEFAULT_CF), Bytes.toBytes(HBASE_SIGNAL_COL), Bytes.toBytes(input(HBASE_SIGNAL_COL)))
    cPut.addColumn(Bytes.toBytes(HBASE_DEFAULT_CF), Bytes.toBytes(VALUE_COL), Bytes.toBytes(input(VALUE_COL)))
    cPut.addColumn(Bytes.toBytes(HBASE_DEFAULT_CF), Bytes.toBytes(VALUE_TYPE_COL), Bytes.toBytes(input(VALUE_TYPE_COL)))
    cPut.addColumn(Bytes.toBytes(HBASE_DEFAULT_CF), Bytes.toBytes(HBASE_STATUS_COL), Bytes.toBytes(input(HBASE_STATUS_COL)))
    hCounter.put(cPut)
    hCounter.close
    connection.close
  }

}
