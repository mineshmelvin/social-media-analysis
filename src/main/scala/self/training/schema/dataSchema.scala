package self.training.schema

import org.json4s.JArray

case class RSVPData(venue: Option[VenueDetails], visibility: String, response: String, guests: Int,
                    member: MemberDetails, rsvp_id: Long, mtime: Long, event: EventDetails,
                    group: GroupDetails)
case class VenueDetails(venue_name: String, lon: Double, lat: Double, venue_id: Int)
case class EventDetails(event_name: String, event_time: Option[Long], event_url: String)
case class GroupDetails(group_city: String, group_country: String, group_id: Int, group_name: String,
                        group_lon: Int, group_lat: Int, group_state: Option[String], group_topics: JArray)
case class MemberDetails(member_id: Long, member_name: String)

case class RSVPDetails(rsvp_id: Long, event_name: String, event_time: Option[Long], event_url: String,
                           group_city: String, group_country: String, group_id: Int, group_lat: Double,
                           group_lon: Double, group_name: String, group_state: Option[String],
                           group_topic_names: List[String], guests: Int, member_id: Long,
                           member_name: String, response: String, mtime: Long, venue_id: Option[Int],
                           venue_lat: Option[Double], venue_lon: Option[Double], venue_name: Option[String],
                           visibility: String)

