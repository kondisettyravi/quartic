package com.quartic.rulesengine.common

import kafka.message.MessageAndMetadata
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.dstream.InputDStream
import kafka.common.TopicAndPartition
import org.apache.spark.streaming.StreamingContext
import kafka.serializer.StringDecoder
import constants.KAFKA_BROKERS

object utilities {
    def getKafkaDirectStreamWithOffset(ssc: StreamingContext, topicsAndOffsets: Map[TopicAndPartition, Long], topic: String, groupID: String = "LmsEnrichments"): InputDStream[(String, String)] = {
    val kafkaParams = Map[String, String]("metadata.broker.list" -> KAFKA_BROKERS, "group.id" -> groupID);
    if (topicsAndOffsets.isEmpty) {
      System.out.println("Creating new stream")
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, Set(topic))
    } else {
      System.out.println("Resuming stream: " + topicsAndOffsets)
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
        ssc, kafkaParams, topicsAndOffsets, (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message))
    }
  }
}