package stm

class RouteLookup(routes: List[Route]) {
  private val lookupTable: Map[String, Route] = routes.map(route => route.route_id -> route).toMap
  def lookup(routeId: String): Route = lookupTable.getOrElse(routeId, null)
}