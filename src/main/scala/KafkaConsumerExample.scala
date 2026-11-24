import java.time.Duration
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.{
  ByteArrayDeserializer,
  StringDeserializer
}

import scala.jdk.CollectionConverters._

object KafkaConsumerExample {

  def main(args: Array[String]): Unit = {

    val props = new Properties()

    props.put(
      "bootstrap.servers",
      "localhost:9092"
    )

    props.put(
      "group.id",
      java.util.UUID.randomUUID().toString
    )

    props.put(
      "key.deserializer",
      classOf[StringDeserializer].getName
    )

    props.put(
      "value.deserializer",
      classOf[ByteArrayDeserializer].getName
    )

    props.put(
      "auto.offset.reset",
      "earliest"
    )

    val consumer =
      new KafkaConsumer[String, Array[Byte]](props)

    consumer.subscribe(
      java.util.Arrays.asList("test-topic")
    )

    println("Consumer Started...")

    while (true) {

      val records =
        consumer.poll(Duration.ofSeconds(1))

      println(
        s"Partitions = ${consumer.assignment()}"
      )

      println(
        s"Records Received = ${records.count()}"
      )

      for (record <- records.asScala) {

        println(
          s"""
             |Topic      : ${record.topic()}
             |Partition  : ${record.partition()}
             |Offset     : ${record.offset()}
             |Key        : ${record.key()}
             |Bytes Size : ${record.value().length}
             |------------------------------------
             |""".stripMargin
        )
      }
    }
  }
}