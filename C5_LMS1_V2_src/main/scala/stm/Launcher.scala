package stm

import scala.io.{BufferedSource, Source}


object Launcher extends App {
  /**
   * 1. Read route and create route lookup table
   * 2. Read calendar and create calendar lookup table
   * 3. Read trip and enrich with route and calendar
   */
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

  val tripSource = Source.fromFile("input/trip-2000.txt")
  val enrichedTrip = tripSource.getLines().toList
    .tail
    .map(_.split(",", -1))
    .map(trip => Trip(trip(0), trip(1), trip(2), trip(3), trip(4), trip(5), trip(6), trip(7), trip(8)))
    .map(trip => TripRoute(trip, routeLookup.lookup(trip.route_id)))
    .map(tripRoute => EnrichedTrip(tripRoute.trip, tripRoute.route, calendarLookup.lookup(tripRoute.trip.service_id)))
  tripSource.close()



  val result = enrichedTrip.filter(_.calendar.monday).filter(_.route.route_type == 1)
  println(enrichedTrip.length)
  println(result.length)
  result.map(_.trip) foreach println
  println(calendars)
  println(enrichedTrip)

   // this could be converted to CSV

  /**
   * @return true if the input is "1" otherwise false
   */
  def binaryToBoolean(in: String): Boolean = if (in.toInt == 1) true else false
}
