package example

import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.{FileIO, Framing, Source}
import akka.stream.{Materializer, SystemMaterializer}
import io.circe.syntax._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer.ProducerConfig

import scala.concurrent.{Await, ExecutionContextExecutor}
import JsonCodecs._
import akka.util.ByteString
import example.CsvToJsonStream.{inputFilePath, parseOrder}

import java.nio.file.Paths
import scala.concurrent.duration.{Duration, DurationInt} // Import the JsonCodecs object for JSON encoding

object ProducerDemoStream extends App {

  implicit val system: ActorSystem = ActorSystem("KafkaProducerSystem")
  implicit val materializer: Materializer = SystemMaterializer(system).materializer
  implicit val ec: ExecutionContextExecutor = system.dispatcher
  import JsonCodecs._

  val inputFilePath = Paths.get("src/main/resources/orders_100k.csv")
  // Kafka producer settings
  private val bootstrapServers: String = Option(System.getenv("KAFKA_BOOTSTRAP_SERVERS")).getOrElse("localhost:9092")

  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers(bootstrapServers)
    .withProperty(ProducerConfig.ACKS_CONFIG, "all")
    .withProperty("enable.idempotence", "false")

  def parseOrder(line: String): Option[Order] = {
    val fields = line.split(";").map(_.trim) // Split and trim fields

    // Ensure we have at least 3 fields (orderId, valueWithoutTaxes, countryCode)
    if (fields.length >= 3) {
      val orderId = fields(0)
      val valueWithoutTaxes = fields(1).toDouble
      val countryCode = fields(2)

      // Handle optional state and totalAmount fields
      val state = if (fields.isDefinedAt(3) && fields(3).nonEmpty) Some(fields(3)) else None
      val totalAmount = if (fields.isDefinedAt(4) && fields(4).nonEmpty) Some(fields(4).toDouble) else None

      // Return the parsed Order object
      Some(Order(orderId, valueWithoutTaxes, countryCode, state, totalAmount))
    } else {
      None
    }
  }

  val source: Source[Order, _] = FileIO.fromPath(inputFilePath)
    .via(Framing.delimiter(ByteString("\n"), 1024, allowTruncation = true)) // Split bytes into lines
    .map(_.utf8String.trim) // Convert ByteString to String
    .drop(1) // Skip header line
    .map(parseOrder) // Parse CSV line into Order object
    .collect { case Some(order) => order } // Filter out invalid lines


  // Create a source of ProducerRecords with the format: key:value (e.g., "5:{"orderId":...}")
  val kafkaProducerSource = source.zipWithIndex.map { case (order, idx) =>
    val orderJson = order.asJson.noSpaces // Serialize the Order to JSON using implicit encoder
    val key = (idx + 1).toString
    new ProducerRecord[String, String]("test-topic", key, orderJson)
  }

  // Send the messages to Kafka using the producer
  kafkaProducerSource
    //.throttle(1, 1.second) // Throttle to produce one message per second (for demo purposes)
    .runWith(Producer.plainSink(producerSettings))
    .onComplete {
      case scala.util.Success(_) =>
        println("Messages successfully sent to Kafka.")
      case scala.util.Failure(e) =>
        println(s"Failed to send messages: ${e.getMessage}")
    }
  Await.result(system.whenTerminated, Duration.Inf)
}