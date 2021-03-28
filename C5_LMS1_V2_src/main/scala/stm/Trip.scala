package stm

// import stm.Consumer.binaryToBoolean

case class Trip(route_id: String,
                service_id: String,
                trip_id: String,
                trip_headsign: String,
                direction_id: String,
                shape_id: String,
                wheelchair_accessible: String,
                note_fr: String,
                note_en: String)

/** Companion object */
object Trip {
  def applyTrip(csv: String): Trip = {
    val a = csv.split(",", -1)
    Trip(a(0), a(1), a(2),a(3), a(4), a(5), a(6), a(7), a(8))

  }

  def toCsv(trip: Trip): String = {
    trip.route_id + "," +
      trip.service_id + "," +
      trip.trip_id + "," +
      trip.trip_headsign + "," +
      trip.direction_id + "," +
      trip.shape_id + "," +
      trip.wheelchair_accessible + "," +
      trip.note_fr + "," +
      trip.note_en
  }
}
