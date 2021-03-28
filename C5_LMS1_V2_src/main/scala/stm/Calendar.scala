package stm

case class Calendar(service_id: String,
                    monday: Boolean,
                    tuesday: Boolean,
                    wednesday: Boolean,
                    thursday: Boolean,
                    friday: Boolean,
                    saturday: Boolean,
                    sunday: Boolean,
                    start_date: String,
                    end_date: String)
