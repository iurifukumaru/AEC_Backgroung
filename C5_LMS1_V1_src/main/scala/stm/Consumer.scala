package stm

import org.apache.kafka.common.serialization.{IntegerDeserializer, IntegerSerializer, StringDeserializer, StringSerializer}

import java.time.Duration
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.collection.JavaConverters._

object Consumer extends App {

  val consumerProperties = new Properties()
  consumerProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  consumerProperties.setProperty(GROUP_ID_CONFIG, "consumer2")
  consumerProperties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, classOf[IntegerDeserializer].getName)
  consumerProperties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProperties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest")

  val consumer = new KafkaConsumer[Int, String](consumerProperties)
  consumer.subscribe(List("trips-v2").asJava)

  /** Not need to instantiate producer for each trip */
  val topicName = "enriched_trip-v1"
  val producerProperties = new Properties()
  producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName)
  producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  val producer = new KafkaProducer[Int, String](producerProperties)

  println("| Key | Value | Partition | Offset |")
  while (true) {
    val polledRecords: ConsumerRecords[Int, String] = consumer.poll(Duration.ofSeconds(1))
    if (!polledRecords.isEmpty) {
      println(s"Polled ${polledRecords.count()} records")
      val recordIterator = polledRecords.iterator()
      while (recordIterator.hasNext) {
        val record = recordIterator.next()
        println(s"| ${record.key()} | ${record.value()} | ${record.partition()} | ${record.offset()} |")

        /** Creating trip object #1 */
        val tripSource = record.value().split(",", -1)
        val tripList = Trip(tripSource(0).toInt, tripSource(1), tripSource(2),
          tripSource(3), tripSource(4).toInt, tripSource(5).toInt, binaryToBoolean(tripSource(6)),
          if (tripSource(7).isEmpty) None else Some (tripSource(7)),
          if (tripSource(8).isEmpty) None else Some (tripSource(8)))
        println(tripList)
        val enrichedTrip = Trip.toCsv(tripList)
        println(enrichedTrip)

        producer.send(new ProducerRecord[Int, String](topicName, 10, s"${enrichedTrip}"))
        producer.flush()

      }
    }
  }
  def binaryToBoolean(in: String): Boolean = if (in.toInt == 1) true else false

}

