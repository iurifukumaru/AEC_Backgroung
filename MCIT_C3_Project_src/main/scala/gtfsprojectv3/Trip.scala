package gtfsprojectv3

case class Trip(
                 route_id: Int,
                 service_id: String,
                 trip_id: String,
                 trip_headsign: String,
                 direction_id: Int,
                 shape_id: Int,
                 wheelchair_accessible: Boolean,
                 note_fr: Option[String],
                 note_en: Option[String]
               )

object Trip {
  def apply(csvLine: String): Trip = {
    val t: Array[String] = csvLine.split(",",-1)
    new Trip(t(0).toInt, t(1), t(2), t(3), t(4).toInt, t(5).toInt, binaryToBoolean(t(6)),
      if (t(7).isEmpty) None else Some (t(7)), if (t(8).isEmpty) None else Some (t(8)))
  }

  def toCsv(trip: Trip): String = {
    trip.route_id + "," +
      trip.service_id + "," +
      trip.trip_id + "," +
      trip.trip_headsign + "," +
      trip.direction_id + "," +
      trip.shape_id + "," +
      trip.wheelchair_accessible + "," +
      trip.note_fr.getOrElse("") + "," +
      trip.note_en.getOrElse("")
  }

  def binaryToBoolean(in: String): Boolean = if (in.toInt == 1) true else false

}