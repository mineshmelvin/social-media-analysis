package self.training

import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import self.training.config.propertiesLoader.loadProperties
import self.training.schema.RSVPDetails

object MeetupStreamAnalysis extends App {

  private val configFilePath = "C:\\Users\\mines\\workspace\\projects\\training\\SocialMediaAnalysis\\src\\main\\resources\\application.conf"
  val properties = loadProperties(configFilePath)

  val spark = SparkSession.builder()
    .appName("Meetup Streaming Analysis")
    .config("spark.hadoop.yarn.resourcemanager.address", properties.getProperty("spark.hadoop.yarn.resourcemanager.address"))
    .config("spark.cassandra.connection.host", properties.getProperty("spark.cassandra.connection.host"))
    .config("spark.cassandra.auth.username", properties.getProperty("spark.cassandra.auth.username"))
    .config("spark.cassandra.auth.password", properties.getProperty("spark.cassandra.auth.password"))
    .getOrCreate()

  import spark.implicits._

  // Read from Kafka as a stream using structured streaming API
  private val kafkaStream = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", properties.getProperty("kafka.bootstrap.servers"))
    .option("subscribe", properties.getProperty("kafka.input.meetup.topic"))
    .option("startingOffsets", "latest")
    .load()

  private val stringStream = kafkaStream.selectExpr("CAST(value AS STRING)").as[String]

  implicit val rsvpEncoder: Encoder[RSVPDetails] = Encoders.product[RSVPDetails]

  // Parse the JSON data into Dataset[RSVPDetailsTest]
  private val rsvpDataset = stringStream.select(from_json($"value", Encoders.product[RSVPDetails].schema).as[RSVPDetails])

  // Perform filtering or other transformations on Dataset
  private val filteredRSVPs = rsvpDataset.filter(_.response == "yes")

  // Write the result to Cassandra or any other sink
  filteredRSVPs.writeStream
    .outputMode("append")
    .format("org.apache.spark.sql.cassandra")
    .option("keyspace", properties.getProperty("cassandra.keyspace"))
    .option("table", properties.getProperty("cassandra.rsvp-data.table"))
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start()
    .awaitTermination()
}
