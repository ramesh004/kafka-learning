package consumermysql

import java.sql.DriverManager
import java.time.Duration
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import scala.jdk.CollectionConverters._

object KafkaToMySQLConsumer {

  def main(args: Array[String]): Unit = {

    // MySQL Connection
    Class.forName("com.mysql.cj.jdbc.Driver")

    val mysqlUrl =
      "jdbc:mysql://localhost:3306/ramesh"

    val mysqlUser = "root"
    val mysqlPassword = "Root@123"

    val conn =
      DriverManager.getConnection(
        mysqlUrl,
        mysqlUser,
        mysqlPassword
      )

    val insertStmt =
      conn.prepareStatement(
        """
          INSERT INTO emp_two(age, state)
          VALUES (?, ?)
        """
      )

    // Kafka Consumer Config
    val props = new Properties()

    props.put(
      "bootstrap.servers",
      "localhost:9092"
    )

    props.put(
      "group.id",
      "mysql-consumer-group"
    )

    props.put(
      "key.deserializer",
      classOf[StringDeserializer].getName
    )

    props.put(
      "value.deserializer",
      classOf[StringDeserializer].getName
    )

    props.put(
      "auto.offset.reset",
      "earliest"
    )

    val consumer =
      new KafkaConsumer[String, String](props)

    consumer.subscribe(
      java.util.Arrays.asList("mysql")
    )

    println("Kafka Consumer Started...")

    while (true) {

      val records =
        consumer.poll(Duration.ofMillis(1000))

      for (record <- records.asScala) {

        try {

          val json =
            record.value()

          val age =
            """"age":(\d+)""".r
              .findFirstMatchIn(json)
              .map(_.group(1).toInt)
              .getOrElse(0)

          val state =
            """"state":"([^"]+)"""".r
              .findFirstMatchIn(json)
              .map(_.group(1))
              .getOrElse("UNKNOWN")

          insertStmt.setInt(1, age)
          insertStmt.setString(2, state)

          insertStmt.executeUpdate()

          println(
            s"""
               |Inserted Record
               |Age   : $age
               |State : $state
               |""".stripMargin
          )

        } catch {

          case ex: Exception =>
            println(
              s"Failed to process Kafka record: ${record.value()}"
            )
            ex.printStackTrace()
        }
      }
    }
  }
}