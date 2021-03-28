package stm

case class Route(
                  route_id: Int,
                  agency_id: String,
                  route_short_name: Int,
                  route_long_name: String,
                  route_type: Int,
                  route_url: String,
                  route_color: String,
                  route_text_color: Option[String]
                )

object Route {
  def apply(csvLine: String): Route = {
    val r: Array[String] = csvLine.split(",",-1)
    new Route(r(0).toInt, r(1), r(2).toInt, r(3), r(4).toInt, r(5), r(6), if (r(7).isEmpty) None else Some (r(7)))
  }

  def toCsv(route: Route): String = {
    route.route_id + "," +
      route.agency_id + "," +
      route.route_short_name + "," +
      route.route_long_name + "," +
      route.route_type + "," +
      route.route_url + "," +
      route.route_color + "," +
      route.route_text_color.getOrElse("")
  }
}
