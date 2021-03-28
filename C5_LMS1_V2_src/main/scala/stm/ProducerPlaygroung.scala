package stm

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}

object ProducerPlaygroung extends App {

  val topicName = "enriched_trip-v1"
  val producerProperties = new Properties()
  producerProperties.setProperty(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
  )
  producerProperties.setProperty(
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName
  )
  producerProperties.setProperty(
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName
  )

  val producer = new KafkaProducer[Int, String](producerProperties)


  producer.send(new ProducerRecord[Int, String](topicName, 10, "70, S1"))
  producer.send(new ProducerRecord[Int, String](topicName, 20, "80, S2"))
  producer.send(new ProducerRecord[Int, String](topicName, 30, "90, S3"))
  producer.send(new ProducerRecord[Int, String](topicName, 40, "100, S4"))
  producer.send(new ProducerRecord[Int, String](topicName, 50, "110, S5"))
  producer.send(new ProducerRecord[Int, String](topicName, 60, "120, S6"))

  producer.flush()

}
