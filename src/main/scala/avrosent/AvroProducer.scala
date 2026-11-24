package AvroProducer

import java.io.ByteArrayOutputStream
import java.util.Properties

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumWriter}
import org.apache.avro.io.EncoderFactory

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer

object AvroProducer {

  def main(args: Array[String]): Unit = {

    val schemaStr =
      """
      {
        "type":"record",
        "name":"Employee",
        "fields":[
          {"name":"id","type":"int"},
          {"name":"name","type":"string"}
        ]
      }
      """

    val schema = new Schema.Parser().parse(schemaStr)

    val employee = new GenericData.Record(schema)
    employee.put("id", 101)
    employee.put("name", "Ramesh")

    // Convert Avro record to byte array
    val writer = new GenericDatumWriter[GenericData.Record](schema)

    val outputStream = new ByteArrayOutputStream()

    val encoder =
      EncoderFactory.get.binaryEncoder(outputStream, null)

    writer.write(employee, encoder)

    encoder.flush()

    val avroBytes = outputStream.toByteArray

    outputStream.close()

    val props = new Properties()

    props.put(
      "bootstrap.servers",
      "localhost:9092"
    )

    props.put(
      "key.serializer",
      classOf[StringSerializer].getName
    )

    props.put(
      "value.serializer",
      classOf[ByteArraySerializer].getName
    )

    val producer =
      new KafkaProducer[String, Array[Byte]](props)

    val record =
      new ProducerRecord[String, Array[Byte]](
        "test-topic",
        "emp101",
        avroBytes
      )

    producer.send(record)

    producer.flush()

    producer.close()

    println("Avro message sent successfully")
  }
}