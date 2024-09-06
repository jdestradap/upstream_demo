package example

import akka.actor.{ActorSystem, Cancellable}
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.{FileIO, Framing, Source}
import akka.stream.{Materializer, SystemMaterializer}
import io.circe.syntax._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer.ProducerConfig
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import akka.util.ByteString
import java.nio.file.Paths
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import akka.Done

import JsonCodecs._

object ProducerSchedulerDemoStream extends App {

  implicit val system: ActorSystem = ActorSystem("KafkaProducerSystem")
  implicit val materializer: Materializer = SystemMaterializer(system).materializer
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  val inputFilePath = Paths.get("src/main/resources/orders.csv")

  // Kafka producer settings
  private val bootstrapServers: String = Option(System.getenv("KAFKA_BOOTSTRAP_SERVERS")).getOrElse("localhost:9092")

  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers(bootstrapServers)
    .withProperty(ProducerConfig.ACKS_CONFIG, "all")
    .withProperty("enable.idempotence", "false")

  // Parsing a line from CSV to an Order object
  def parseOrder(line: String): Option[OrderNew] = {
    val fields = line.split(";").map(_.trim)

    if (fields.length >= 3) {
      val orderId = fields(0)
      val valueWithoutTaxes = fields(1)
      val countryCode = fields(2)
      val state = if (fields.isDefinedAt(3) && fields(3).nonEmpty) Some(fields(3)) else None
      val totalAmount = if (fields.isDefinedAt(4) && fields(4).nonEmpty) Some(fields(4).toDouble) else None

      Some(OrderNew(orderId, valueWithoutTaxes, countryCode, state, totalAmount))
    } else {
      None
    }
  }

  private def processCsvFile(): Future[Done] = {
    val source: Source[OrderNew, _] = FileIO.fromPath(inputFilePath)
      .via(Framing.delimiter(ByteString("\n"), 1024, allowTruncation = true))
      .map(_.utf8String.trim)
      .drop(1)
      .map(parseOrder)
      .collect { case Some(order) => order }

    // Source of ProducerRecords to send to Kafka
    val kafkaProducerSource = source.zipWithIndex.map { case (order, idx) =>
      val orderJson = order.asJson.noSpaces
      val key = (idx + 1).toString
      new ProducerRecord[String, String]("test-topic", key, orderJson)
    }

    // Run the stream and return the Future[Done]
    kafkaProducerSource
      .runWith(Producer.plainSink(producerSettings))
  }

  // Schedule to run processCsvFile every 10 seconds
  val cancellable: Cancellable = system.scheduler.scheduleWithFixedDelay(
    0.seconds, 10.seconds)(() => {
    val result = processCsvFile()
    result.onComplete {
      case Success(_) =>
        println("Messages successfully sent to Kafka.")
      case Failure(e) =>
        println(s"Failed to send messages: ${e.getMessage}")
    }
  })

  // Shutdown the system after all messages are sent
  sys.addShutdownHook {
    cancellable.cancel()
    Await.result(system.terminate(), 30.seconds)
  }

  // Keep the application running indefinitely
  Await.result(system.whenTerminated, Duration.Inf)
}
