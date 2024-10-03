package self.training

import org.apache.spark.sql.{Dataset, SparkSession}
import self.training.config.propertiesLoader.loadProperties
import self.training.schema.RSVPDetails

/**
 * @author ${Minesh.Melvin}
 */
object MeetupBatchAnalysis extends App {

  val configFilePath = "C:\\Users\\mines\\workspace\\projects\\training\\SocialMediaAnalysis\\src\\main\\resources\\application.conf"
  val properties = loadProperties(configFilePath)
  val spark = SparkSession.builder()
    .master("local")
    .appName("Meetup Batch Analysis")
    .config("spark.hadoop.yarn.resourcemanager.address", properties.getProperty("spark.hadoop.yarn.resourcemanager.address"))
    .config("spark.cassandra.connection.host", properties.getProperty("spark.cassandra.connection.host"))
    .config("spark.cassandra.auth.username", properties.getProperty("spark.cassandra.auth.username"))
    .config("spark.cassandra.auth.password", properties.getProperty("spark.cassandra.auth.password"))
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  // Read data from Cassandra Keyspace and table
  val rsvp: Dataset[RSVPDetails] = spark.read.format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> properties.getProperty("cassandra.rsvp-data.table"), "keyspace" -> properties.getProperty("cassandra.keyspace")))
    .load()
    .as[RSVPDetails]

  rsvp.cache()


  // Query 4 : Total Count of distinct cities
  val group_city_data = rsvp.select("group_city")

  //// Overall no.of of unique group cities
  // Get distinct group_city names
  val groupCityOverall = group_city_data.map(city => city.getAs[String]("group_city")).distinct()

  /// Calculate count for Count  each group city. ex: Sydney - 1256, London - 2456
  // This is like word count problem in Spark
  val groupCityCounts = group_city_data
    .map(city => (city.getAs[String]("group_city")).reduceByKey(_+_)

}
