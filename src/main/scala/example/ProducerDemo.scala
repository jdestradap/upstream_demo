package example

import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.Source
import akka.stream.{Materializer, SystemMaterializer}
import io.circe.syntax._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer.ProducerConfig

import scala.concurrent.ExecutionContextExecutor
import JsonCodecs._

import scala.concurrent.duration.DurationInt // Import the JsonCodecs object for JSON encoding

object ProducerDemo extends App {

  implicit val system: ActorSystem = ActorSystem("KafkaProducerSystem")
  implicit val materializer: Materializer = SystemMaterializer(system).materializer
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  // Kafka producer settings
  private val bootstrapServers: String = Option(System.getenv("KAFKA_BOOTSTRAP_SERVERS")).getOrElse("localhost:9092")

  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers(bootstrapServers)
    .withProperty(ProducerConfig.ACKS_CONFIG, "all")
    .withProperty("enable.idempotence", "false")

  // Define orders
  val orders = List(
    Order(orderId = "67890", valueWithoutTaxes = 299.99, countryCode = "CA", state = None, totalAmount = None),
    Order(orderId = "12345", valueWithoutTaxes = 199.99, countryCode = "US", state = Some("CA"), totalAmount = None)
  )

  // Create a source of ProducerRecords with the format: key:value (e.g., "5:{"orderId":...}")
  val kafkaProducerSource = Source(orders.zipWithIndex).map { case (order, idx) =>
    val orderJson = order.asJson.noSpaces // Serialize the Order to JSON using implicit encoder
    val key = (idx + 1).toString
    new ProducerRecord[String, String]("test-topic", key, orderJson)
  }

  // Send the messages to Kafka using the producer
  kafkaProducerSource
    .throttle(1, 1.second) // Throttle to produce one message per second (for demo purposes)
    .runWith(Producer.plainSink(producerSettings))
    .onComplete {
      case scala.util.Success(_) =>
        println("Messages successfully sent to Kafka.")
        system.terminate()
      case scala.util.Failure(e) =>
        println(s"Failed to send messages: ${e.getMessage}")
        system.terminate()
    }
}