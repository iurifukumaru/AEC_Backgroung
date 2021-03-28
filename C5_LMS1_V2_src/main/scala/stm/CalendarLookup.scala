package stm

class CalendarLookup(calendars: List[Calendar]) {
  private val lookupTable: Map[String, Calendar] =
    calendars.map(calendar => calendar.service_id -> calendar).toMap
  def lookup(serviceId: String): Calendar = lookupTable.getOrElse(serviceId, null)
}