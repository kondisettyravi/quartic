package com.quartic.rulesengine.common

import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object constants {
  val HDFS_CONSTANTS_FILE_LOC = ""
  val hdfsConf = new Configuration()
  val fs = FileSystem.get(hdfsConf)
  val hdfsFilePath = HDFS_CONSTANTS_FILE_LOC
  val is = fs.open(new Path(hdfsFilePath))
  val prop = new Properties()
  prop.load(is)
  is.close()

//  Constants
  val APP_NAME = "RULES ENGINE"
  val APP_ID = "RULES ENGINE"
  val KAFKA_STREAM_DURATION = 2
  val HBASE_COL_PARTITION = "kafka_partition"
  val HBASE_COL_TOPIC= "topic"
  val HBASE_COL_NEXT = "next_pos"
  val HBASE_CHECKPOINT_TABLE = "quartic_checkpoint"
  val VALUE_TYPE_COL = "value_type"
  val VALUE_COL = "value"
  val VIOLATION = "violated"
  val WITHIN_LIMITS = "not violated"
  val HBASE_STATUS_COL = "status"
  val INTEGER_STRING = "INTEGER"
  val STRING_VALUE = "STRING"
  val LOW_VALUE = "LOW"
  val DATETIME_VALUE = "DATETIME"
  val DEFAULT_VALUE = "_"
  val HBASE_OUT_TABLE = "quartic_rules_data"
  val HBASE_SIGNAL_COL = "signal"
//  Mandatory properties
  val KAFKA_BROKERS = prop.getProperty("BROKERS_LIST")
  val INPUT_TOPIC = prop.getProperty("INPUT_TOPIC")

//  Custom properties
  val HBASE_CLIENT_RETRIES = prop.getOrDefault("HBASE_CLIENT_RETRIES", "50").asInstanceOf[String]
  val HBASE_DEFAULT_CF = prop.getOrDefault("DEFAULT_COLUMN_FAMILY", "default").asInstanceOf[String]

}