package com.quartic.rulesengine.problem

import java.sql.PreparedStatement
import java.sql.ResultSet
import org.joda.time.format.DateTimeFormat
import org.joda.time._
import scala.util.parsing.json.JSON
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.kafka.OffsetRange
import kafka.common.TopicAndPartition
import com.quartic.rulesengine.common.utilities.getKafkaDirectStreamWithOffset
import com.quartic.rulesengine.common.constants._
import com.quartic.rulesengine.common.constants.HBASE_CHECKPOINT_TABLE
import com.quartic.rulesengine.common.hbasehelper


object engine {
  def main(args: Array[String]): Unit = {
    
    def runRulesOnInputAndSave(jsonString: String): Unit = {
      val jsonMap = JSON.parseFull(jsonString).get.asInstanceOf[Map[String, String]]
      val status = jsonMap(VALUE_TYPE_COL).toString.toUpperCase match {
        case INTEGER_STRING  => if (jsonMap(VALUE_COL).toString.toFloat > 240) VIOLATION else WITHIN_LIMITS
        case STRING_VALUE    => if (jsonMap(VALUE_COL).toString == LOW_VALUE) VIOLATION else WITHIN_LIMITS
        case DATETIME_VALUE  => if (!DateTimeFormat.forPattern("dd/MM/YYYY HH:mm:ss.SSSS").parseDateTime(jsonMap(VALUE_COL).toString).isAfter(new DateTime())) VIOLATION else WITHIN_LIMITS
        case DEFAULT_VALUE   => VIOLATION
      }
      val outMap = jsonMap + (HBASE_STATUS_COL -> status)
      hbasehelper.writeToHbase(outMap, HBASE_OUT_TABLE)
    }

  def createStreamingContext(): StreamingContext = {
      println("STARTING JOB")

      val sparkConf = new SparkConf().setAppName(APP_NAME)
      sparkConf.set("spark.app.id", APP_ID)

      val sc = new SparkContext(sparkConf)

      val interval = Seconds(KAFKA_STREAM_DURATION) //KAFKA_STREAM_DURATION
      val ssc = new StreamingContext(sc, interval)

      @transient val hbaseConfig = hbasehelper.getHbaseConfiguration()
      hbasehelper.createTableIfNotExists(HBASE_CHECKPOINT_TABLE, hbaseConfig)
      hbaseConfig.set(TableInputFormat.INPUT_TABLE, HBASE_CHECKPOINT_TABLE)

      @transient val topicsAndOffsets: scala.collection.immutable.Map[TopicAndPartition, Long] =
        sc.newAPIHadoopRDD(hbaseConfig, classOf[TableInputFormat],
          classOf[ImmutableBytesWritable],
          classOf[Result])
          .map(x => x._2)
          .map(result => (
            TopicAndPartition(
              hbasehelper.getValue(result, HBASE_DEFAULT_CF, HBASE_COL_TOPIC),
              hbasehelper.getValueAsInt(result, HBASE_DEFAULT_CF, HBASE_COL_PARTITION)),
              hbasehelper.getValueAsLong(result, HBASE_DEFAULT_CF, HBASE_COL_NEXT)))
          .collectAsMap.toMap

      val dstreamWithOffset = getKafkaDirectStreamWithOffset(ssc, topicsAndOffsets, INPUT_TOPIC);

      dstreamWithOffset.foreachRDD { rdd =>
        /*Executes at workers*/
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.foreachPartition { sparkPartition =>

          if (!sparkPartition.isEmpty) {
            println("RECEIVED DATA IN THIS BATCH")
            val currPartitionid = TaskContext.get.partitionId
            val osr: OffsetRange = offsetRanges(currPartitionid)
            val begin = osr.fromOffset

            val partition = osr.partition
            val topic = osr.topic
            val untilOffset = osr.untilOffset //exclusive
            val stm = System.currentTimeMillis()

            sparkPartition.foreach { arr =>
              try {
                val jsonString = arr._2
                runRulesOnInputAndSave(jsonString)
                ("PROCESSED MESSAGE IN JOB")
              } catch {
                case e: kafka.common.FailedToSendMessageException => {
                  e.printStackTrace();
                  println(arr._2, e.getClass().getCanonicalName() + " occured : " + e.getMessage + " TRACE: " + ExceptionUtils.getStackTrace(e), "Unable to process Message", true, true)
                }

                case e: java.lang.ClassCastException => {
                  e.printStackTrace();
                  println(arr._2, e.getClass().getCanonicalName() + " occured : " + e.getMessage + " TRACE: " + ExceptionUtils.getStackTrace(e), "Unable to process Message", true, false)
                }

                case e: Exception =>
                  println("Exception Occured")
                  e.printStackTrace()
                  println(arr._2, e.getClass().getCanonicalName() + " occured : " + e.getMessage + " TRACE: " + ExceptionUtils.getStackTrace(e), "Unable to process Message", true, true)
              }
            }
            hbasehelper.commitOffset(HBASE_CHECKPOINT_TABLE, partition, topic, untilOffset)
          } else {
            println("NO DATA RECEIVED IN THIS BATCH")
          }
        }
      }

      ssc
    }

    var ssc: StreamingContext = null
    try {
      ssc = createStreamingContext
      ssc.start()
      ssc.awaitTermination()
    } finally {
      if (ssc != null)
        ssc.stop(stopSparkContext = true, stopGracefully = true)
    }
  
  }
}