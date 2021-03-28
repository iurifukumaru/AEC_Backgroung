package gtfsprojectv3

import java.io.PrintWriter
import org.apache.hadoop.fs.Path

object Launcher extends HDFS {

  if(fs.delete(
    new Path(
    s"$uri/user/winter2020/iuri/course3"),
    true)) println("Folder 'course3' deleted before Instantiate!")
  println(fs.getUri)

  val tripSource = new Path(s"$uri/user/winter2020/iuri/stm/trips.txt")
  val streamTrip = fs.open(tripSource)
  def readLinesTrip = Stream.cons(streamTrip.readLine,
    Stream.continually(streamTrip.readLine)).takeWhile(_!=null)
  val tripList: List[Trip] = readLinesTrip
    .toList
    .tail
    .map(_.split(",", -1))
    .map(t => Trip(t(0).toInt, t(1), t(2), t(3), t(4).toInt, t(5).toInt, binaryToBoolean(t(6)),
      if (t(7).isEmpty) None else Some(t(7)), if (t(8).isEmpty) None else Some(t(8))))
  streamTrip.close()

  val routeSource = new Path(s"$uri/user/winter2020/iuri/stm/routes.txt")
  val streamRoute = fs.open(routeSource)
  def readLinesRoute = Stream.cons(streamRoute.readLine,
    Stream.continually(streamRoute.readLine)).takeWhile(_!=null)
  val routeList: List[Route] = readLinesRoute
    .toList
    .tail
    .map(_.split(",", -1))
    .map(r => Route(r(0).toInt, r(1), r(2).toInt, r(3), r(4).toInt, r(5), r(6), if (r(7).isEmpty) None else Some(r(7))))
  streamRoute.close()

  val calendarSource = new Path(s"$uri/user/winter2020/iuri/stm/calendar.txt")
  val streamCalendar = fs.open(calendarSource)
  def readLinesCalendar = Stream.cons(streamCalendar.readLine,
    Stream.continually(streamCalendar.readLine)).takeWhile(_!=null)
  val calendarList: List[Calendar] = readLinesCalendar
    .toList
    .tail
    .map(_.split(",", -1))
    .map(c => Calendar(
      c(0),
      binaryToBoolean(c(1)),
      binaryToBoolean(c(2)),
      binaryToBoolean(c(3)),
      binaryToBoolean(c(4)),
      binaryToBoolean(c(5)),
      binaryToBoolean(c(6)),
      binaryToBoolean(c(7)),
      c(8).toInt,
      c(9).toInt))
  streamCalendar.close()

  val tripRouteList = new TripRoute [Trip, Route](trip => trip.route_id.toString)(route => route.route_id.toString)
    .join(tripList, routeList)

  val enrichedTripList = new EnrichedTrip[Calendar, JoinClass]((calendar, routetrip)
  => calendar.service_id == routetrip.left.asInstanceOf[Trip].service_id).join(calendarList, tripRouteList)

  val outputFile = fs.create(new Path(s"$uri/user/winter2020/iuri/course3/enrichedTrip.csv"))

  val header: String = "route_id,service_id,trip_id,trip_headsign,direction_id,shape_id,wheelchair_accessible," +
    "note_fr,note_en,route_id,agency_id,route_short_name,route_long_name,route_type,route_url,route_color," +
    "route_text_color,service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date"

  val output = enrichedTripList.map(print => {
    val t = Trip.toCsv(print.right.getOrElse(" ").asInstanceOf[JoinClass].left.asInstanceOf[Trip])
    val r = Route.toCsv(print.right.getOrElse(" ").asInstanceOf[JoinClass].right.getOrElse(" ").asInstanceOf[Route])
    val c = Calendar.toCsv(print.left.asInstanceOf[Calendar])
    t + "," + r + "," + c
  })

  val writer = new PrintWriter(outputFile)
  writer.write(header)

  for (line <- output) {
    writer.write(System.lineSeparator)
    writer.write(line)}
  outputFile.close()

  fs.copyToLocalFile(
    new Path(s"$uri/user/winter2020/iuri/course3/enrichedTrip.csv"),
    new Path("file:///home/iuri/Desktop/copyEnrichedTrip.csv"))

  def binaryToBoolean(in: String): Boolean = if (in.toInt == 1) true else false

}
