package self.training

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.codehaus.jackson.JsonFactory
import org.codehaus.jackson.map.ObjectMapper
import self.training.config.propertiesLoader.loadProperties

import java.net.URL
import java.util.Properties

object MeetupKafkaProducer extends App {

  val configFilePath = "C:\\Users\\mines\\workspace\\projects\\training\\SocialMediaAnalysis\\src\\main\\resources\\application.conf"
  val properties = loadProperties(configFilePath)

  // MeetupA API json generator
  private val url = new URL(properties.getProperty("meetup.url"))
  private val con = url.openConnection()
  con.addRequestProperty("User-Agent",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0)")
  private val jsonFactory = new JsonFactory(new ObjectMapper())
  private val parser = jsonFactory.createJsonParser(con.getInputStream)

  /* Producer Properties */
  private val kafkaProps = new Properties()
  kafkaProps.put("bootstrap.servers", properties.getProperty("kafka.bootstrap.servers"))
  kafkaProps.put("group.id", "None")
  kafkaProps.put("acks", "all")
  kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProps.put("enable.auto.commit", "true")
  kafkaProps.put("auto.commit.interval.ms", "1000")
  kafkaProps.put("session.timeout.ms", "30000")


  private val kafkaProducer = new KafkaProducer[String, String](kafkaProps)
  while (parser.nextToken() != null) {
    val record = parser.readValueAsTree().toString
    println(record)
    val producerRecord = new ProducerRecord[String, String](properties.getProperty("kafka.input.meetup.topic"), record)
    kafkaProducer.send(producerRecord)
  }

  kafkaProducer.close()
}
