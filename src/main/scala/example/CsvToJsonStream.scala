package example

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, IOResult, Materializer, SystemMaterializer}
import akka.stream.scaladsl.{FileIO, Framing, Sink, Source}
import akka.util.ByteString
import example.ProducerDemo.system
import io.circe.syntax._
import io.circe.generic.auto._
import akka.stream.scaladsl.Sink

import java.nio.file.{Path, Paths}
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

object CsvToJsonStream {

  implicit val system: ActorSystem = ActorSystem("CsvToJsonSystem")
  implicit val materializer: Materializer = SystemMaterializer(system).materializer
  implicit val ec: ExecutionContextExecutor = system.dispatcher
  import JsonCodecs._

  val inputFilePath: Path = Paths.get("src/main/resources/orders_100.csv")
  val outputFilePath: Path = Paths.get("src/main/resources/orders.json")

  // Function to parse CSV lines into Order objects
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

  // Create a Sink that writes the orders as JSON to the output file
  val sink: Sink[ByteString, Future[IOResult]] = FileIO.toPath(outputFilePath)

  // Run the stream with a map to convert Orders to JSON and then to ByteString
  val result: Future[IOResult] = source
    .map(order => ByteString(order.asJson.noSpaces + "\n")) // Convert Order to JSON and then to ByteString
    .runWith(sink)

  // Handle the result of the stream processing
  result.onComplete {
    case Success(ioResult) =>
      println(s"Stream processing completed successfully. Bytes written: ${ioResult.count}")
      system.terminate()
    case Failure(exception) =>
      println(s"Stream processing failed with exception: $exception")
      system.terminate()
  }

}