package gtfsprojectv3

case class Calendar(
                     service_id: String,
                     monday: Boolean,
                     tuesday: Boolean,
                     wednesday: Boolean,
                     thursday: Boolean,
                     friday: Boolean,
                     saturday: Boolean,
                     sunday: Boolean,
                     start_date: Int,
                     end_date: Int
                   )

object Calendar {
  def apply(csvLine: String): Calendar = {
    val c: Array[String] = csvLine.split(",",-1)
    new Calendar(
      c(0),
      binaryToBoolean(c(1)),
      binaryToBoolean(c(2)),
      binaryToBoolean(c(3)),
      binaryToBoolean(c(4)),
      binaryToBoolean(c(5)),
      binaryToBoolean(c(6)),
      binaryToBoolean(c(7)),
      c(8).toInt,
      c(9).toInt)
  }

  def toCsv(calendar: Calendar): String = {
    calendar.service_id + "," +
      calendar.monday + "," +
      calendar.tuesday + "," +
      calendar.wednesday + "," +
      calendar.thursday + "," +
      calendar.friday + "," +
      calendar.saturday + "," +
      calendar.sunday + "," +
      calendar.start_date + "," +
      calendar.end_date
  }

  def binaryToBoolean(in: String): Boolean = if (in.toInt == 1) true else false

}
