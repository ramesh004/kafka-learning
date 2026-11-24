package producer

import java.sql.DriverManager
import java.util.Properties

import org.apache.kafka.clients.producer.{
  KafkaProducer,
  ProducerRecord
}
import org.apache.kafka.common.serialization.StringSerializer

object MySQLToKafkaProducer {

  def main(args: Array[String]): Unit = {

    val url =
      "jdbc:mysql://localhost:3306/ramesh"

    val user = "root"
    val password = "Root@123"

    val conn =
      DriverManager.getConnection(
        url,
        user,
        password
      )

    val stmt =
      conn.createStatement()

    val rs =
      stmt.executeQuery(
        "SELECT age, state FROM employee_data"
      )

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
      classOf[StringSerializer].getName
    )

    val producer =
      new KafkaProducer[String, String](props)

    while (rs.next()) {

      val age =
        rs.getInt("age")

      val state =
        rs.getString("state")

      val msg =
        s"""{"age":$age,"state":"$state"}"""

      producer.send(
        new ProducerRecord[String, String](
          "mysql",
          age.toString,
          msg
        )
      )

      println(s"Sent: $msg")
    }

    producer.flush()
    producer.close()

    rs.close()
    stmt.close()
    conn.close()

    println("All records sent to Kafka successfully.")
  }
}