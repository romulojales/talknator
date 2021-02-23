package com.romulojales

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.serialization.StringDeserializer

import scala.jdk.CollectionConverters._
import java.util.Properties

object KafkaReader extends App {

  def getProperties: Properties = {
    val properties = new Properties()
    properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.setProperty(CommonClientConfigs.GROUP_ID_CONFIG, "talknator")
    properties
  }

  def getConsumer(properties: Properties): KafkaConsumer[String, String] = {
    val keyDeserializer   = new StringDeserializer
    val valueDeserializer = new StringDeserializer
    new KafkaConsumer(properties, keyDeserializer, valueDeserializer)
  }

  def consume(consumer: KafkaConsumer[String, String]): Unit = {
    val records = consumer.poll(java.time.Duration.ofMillis(500))

    records.forEach { record =>
      println(s"Message: ${record.value}.  (Partition: ${record.partition}, Offset: ${record.offset})")
    }

    consumer.commitAsync()
  }

  def run(): Unit = {
    val consumer = getConsumer(getProperties)
    consumer.subscribe(Set("quickstart-events").asJava)
    println("Starting consumer")
    while (true) {
      consume(consumer)
    }
    println("Finishing consumer")
    consumer.close()
  }

  run()
}
