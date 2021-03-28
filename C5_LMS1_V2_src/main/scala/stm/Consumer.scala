package stm

import java.time.Duration
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerDeserializer, IntegerSerializer, StringDeserializer, StringSerializer}

import scala.collection.JavaConverters._
import scala.io.{BufferedSource, Source}

object Consumer extends App {

  val routeSource: BufferedSource = Source.fromFile("input/routes.txt")
  val routes: List[Route] = routeSource.getLines().toList
    .tail
    .map(_.split(",", -1))
    .map(route => Route(route(0), route(1), route(2), route(3), route(4).toInt, route(5), route(6), route(7)))
  val routeLookup: RouteLookup = new RouteLookup(routes)
  routeSource.close()

  val calendarSource: BufferedSource = Source.fromFile("input/calendar.txt")
  val calendars = calendarSource.getLines().toList
    .tail
    .map(_.split(",", -1))
    .map(calendar => Calendar(
      calendar(0),
      binaryToBoolean(calendar(1)),
      binaryToBoolean(calendar(2)),
      binaryToBoolean(calendar(3)),
      binaryToBoolean(calendar(4)),
      binaryToBoolean(calendar(5)),
      binaryToBoolean(calendar(6)),
      binaryToBoolean(calendar(7)),
      calendar(8),
      calendar(9))
    )
  val calendarLookup: CalendarLookup = new CalendarLookup(calendars)
  calendarSource.close()

  val consumerProperties = new Properties()
  consumerProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  consumerProperties.setProperty(GROUP_ID_CONFIG, "consumer1")
  consumerProperties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, classOf[IntegerDeserializer].getName)
  consumerProperties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProperties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest")

  val consumer = new KafkaConsumer[Int, String](consumerProperties)
  consumer.subscribe(List("trips-v2").asJava)

  /** Not need to instantiate producer for each trip */
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

  println("| Key | Value | Partition | Offset |")
  while (true) {
    val polledRecords: ConsumerRecords[Int, String] = consumer.poll(Duration.ofSeconds(1))
    if (!polledRecords.isEmpty) {
      println(s"Polled ${polledRecords.count()} records")
      val recordIterator = polledRecords.iterator()
      while (recordIterator.hasNext) {
        val record = recordIterator.next()
        println(s"| ${record.key()} | ${record.value()} | ${record.partition()} | ${record.offset()} |")

        /** Creating trip and enrichedTrip object and sending another topic #1 */
        val tripSource = record.value()
        val tSS = tripSource.split(",", -1)
        val tripList = Trip(tSS(0), tSS(1), tSS(2), tSS(3), tSS(4), tSS(5), tSS(6), tSS(7), tSS(8))
        println(tripList)
        val enrichedTrip = Trip.toCsv(tripList)
        println(enrichedTrip)
//        val enrichedTrip = tripList
//          .map(trip => TripRoute(trip, routeLookup.lookup(trip.route_id)))
//          .map(tripRoute => EnrichedTrip(tripRoute.trip, tripRoute.route, calendarLookup.lookup(tripRoute.trip.service_id)))
//        tripSource.close()




//        producer.send(new ProducerRecord[Int, String](topicName, 10, s"${enrichedTrip}"))
//        producer.flush()

        /** Creating trip object #2 and enrichedtrip object with route and calendar without join
         * correctly */
//        def applyTrip(csv: String): Trip = {
//          val a = csv.split(",", -1)
//          Trip(a(0), a(1), a(2),a(3), a(4), a(5), a(6), a(7), a(8))
//
//        }
//        val parseTrip: Trip = applyTrip(record.value())
//        val j = parseTrip
//        val out = (j.route_id + "," + j.service_id + "," + j.trip_id + "," + j.trip_headsign + "," + j.direction_id
//          + "," + j.shape_id + "," + j.wheelchair_accessible + "," + j.note_en + "," + j.note_fr
//          + "," + calendars + routes)
//        println(out)
//        producer.send(new ProducerRecord[Int, String](topicName, 10, out))
//        producer.flush()

        /** Attempt #2. Create EnrichedTrip.
         * get tripList objects, but .map function not works */
//        val enrichedTrip = tripList
//          .map(trip => TripRoute(trip, routeLookup.lookup(trip.route_id)))
//          .map(tripRoute => EnrichedTrip(tripRoute.trip, tripRoute.route, calendarLookup.lookup(tripRoute.trip.service_id)))

        /** Attempt #3 Create EnrichedTrip.
         * Recreate Trip List, but .split function not works */
//        val tripSource2 = record.value()
//        val enrichedTrip = tripSource2.toList.split(",", -1)
//          .map(trip => Trip(trip(0).toInt, trip(1), trip(2), trip(3), trip(4).toInt, trip(5).toInt, binaryToBoolean(trip(6)), trip(7), trip(8)))
//          .map(trip => TripRoute(trip, routeLookup.lookup(trip.route_id)))
//          .map(tripRoute => EnrichedTrip(tripRoute.trip, tripRoute.route, calendarLookup.lookup(tripRoute.trip.service_id)))

      }
    }
  }
  def binaryToBoolean(in: String): Boolean = if (in.toInt == 1) true else false

}


