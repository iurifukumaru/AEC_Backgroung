package stm

import java.io.PrintWriter

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Main extends HDFS {

  if(fs.delete(new Path(s"$uri/user/winter2020/iuri/course5/assignment2"),true))
    println("Folder 'course 5 - assignment2' deleted before Instantiate!")

  fs.mkdirs(new Path(s"$uri/user/winter2020/iuri/course5/assignment2/trips"))
  fs.mkdirs(new Path(s"$uri/user/winter2020/iuri/course5/assignment2/calendar_dates"))
  fs.mkdirs(new Path(s"$uri/user/winter2020/iuri/course5/assignment2/routes"))
  fs.mkdirs(new Path(s"$uri/user/winter2020/iuri/course5/assignment2/enriched_trip"))

  fs.copyFromLocalFile(new Path("file:///home/iuri/Documents/Course5/trips.txt"),
    new Path(s"$uri/user/winter2020/iuri/course5/assignment2/trips/trips.txt"))
  fs.copyFromLocalFile(new Path("file:///home/iuri/Documents/Course5/calendar_dates.txt"),
    new Path(s"$uri/user/winter2020/iuri/course5/assignment2/calendar_dates/calendar_dates.txt"))
  fs.copyFromLocalFile(new Path("file:///home/iuri/Documents/Course5/routes.txt"),
    new Path(s"$uri/user/winter2020/iuri/course5/assignment2/routes/routes.txt"))

  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("STM Enriched Trip")

  val sc = new SparkContext(sparkConf)

  val trips = sc.textFile("/user/winter2020/iuri/course5/assignment2/trips/trips.txt")
  trips.take(2).foreach(println)
  val tripsRdd: RDD[Trip] = trips.filter(!_.contains("trip_id")).map(Trip.fromCsv)
  tripsRdd.take(2).foreach(println)

  val calendar_dates = sc.textFile("/user/winter2020/iuri/course5/assignment2/calendar_dates")
  calendar_dates.take(3).foreach(println)
  val calendar_datesRdd: RDD[CalendarDate] = calendar_dates.filter(!_.contains("date")).map(CalendarDate.fromCsv)
  calendar_datesRdd.take(3).foreach(println)

  val routes = sc.textFile("/user/winter2020/iuri/course5/assignment2/routes")
  routes.take(4).foreach(println)
  val routesRdd: RDD[Route] = routes.filter(!_.contains("route_id")).map(Route.fromCsv)
  routesRdd.take(4).foreach(println)

  val t: RDD[(String,Trip)] = tripsRdd.keyBy(_.service_id)
  val c: RDD[(String,CalendarDate)] = calendar_datesRdd.keyBy(_.service_id)
  val r: RDD[(Int,Route)] = routesRdd.keyBy(_.route_id)

  val tripCalendars = t.join(c).map {
    case (service_id, (trip, calendar_date)) => CalendarDate.toCsv1(calendar_date, trip)
  }
  tripCalendars.take(5).foreach(println)
  val tripCalendarsRdd: RDD[TripCalendar] = tripCalendars.map(TripCalendar.fromCsv)
  tripCalendarsRdd.take(5).foreach(println)

  val tc: RDD[(Int,TripCalendar)] = tripCalendarsRdd.keyBy(_.route_id)

  val enrichedTrip = tc.join(r).map {
    case (route_id, (tripCalendar, route)) => Route.toCsv2(route, tripCalendar)
  }
  enrichedTrip.take(10).foreach(println)

  val outputFile = fs.create(new Path(
    s"$uri/user/winter2020/iuri/course5/assignment2/enriched_trip/enrichedTrip.csv"))

  val header: String = "route_id,trip_headsign,wheelchair_accessible,date,exception_type,route_long_name,route_color"

  val output = enrichedTrip

  val writer = new PrintWriter(outputFile)
  writer.write(header)

  for (line <- output) {
    writer.write(System.lineSeparator)
    writer.write(line)}
  outputFile.close()

  fs.copyToLocalFile(
    new Path(s"$uri/user/winter2020/iuri/course5/assignment2/enriched_trip/enrichedTrip.csv"),
    new Path("file:///home/iuri/Documents/Course5/copyEnrichedTrip.csv"))

  sc.stop()

}

case class Trip(route_id: Int, service_id: String, trip_id: String,
                trip_headsign: String, wheelchair_accessible: Boolean)
object Trip {
  def fromCsv(trip: String): Trip = {
    val fields = trip.split(",", -1)
    Trip(fields(0).toInt, fields(1), fields(2), fields(3), binaryToBoolean1(fields(6)))
  }
  def binaryToBoolean1(in: String): Boolean = if (in.toInt == 1) true else false
}

case class CalendarDate(service_id: String, date: String, exception_type: Int)
object CalendarDate {
  def fromCsv(calendar_date: String): CalendarDate = {
    val fields = calendar_date.split(",", -1)
    CalendarDate(fields(0), fields(1), fields(2).toInt)
  }
  def toCsv1(calendar_date: CalendarDate, trip: Trip): String = s"${trip.route_id},${trip.trip_headsign}," +
    s"${trip.wheelchair_accessible},${calendar_date.date},${calendar_date.exception_type}"
}

case class TripCalendar(route_id: Int, trip_headsign: String, wheelchair_accessible: String, date: String,
                         exception_type: Int)
object TripCalendar {
  def fromCsv(tripcalendar: String): TripCalendar = {
    val fields = tripcalendar.split(",", -1)
    TripCalendar(fields(0).toInt, fields(1), fields(2), fields(3), fields(4).toInt)
  }
}

case class Route(route_id: Int, route_long_name: String, route_color: String)
object Route {
  def fromCsv(route: String): Route = {
    val fields = route.split(",", -1)
    Route(fields(0).toInt, fields(3), fields(6))
  }
  def toCsv2(route: Route, tripcalendar: TripCalendar): String = s"${tripcalendar.route_id}," +
    s"${tripcalendar.trip_headsign},${tripcalendar.wheelchair_accessible},${tripcalendar.date}," +
    s"${tripcalendar.exception_type},${route.route_long_name},${route.route_color}"
}



