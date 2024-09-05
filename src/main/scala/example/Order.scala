package example

import java.time.Instant
import io.circe._, io.circe.generic.semiauto._
import io.circe.parser._
import io.circe.syntax._

// Define an object to hold implicit decoders
object JsonCodecs {
  implicit val decodeInstant: Decoder[Instant] = Decoder.decodeString.emapTry(str => scala.util.Try(Instant.parse(str)))
  // Automatic derivation of decoders and encoders for Order
  implicit val orderDecoder: Decoder[Order] = deriveDecoder[Order]
  implicit val orderEncoder: Encoder[Order] = deriveEncoder[Order]
}

case class Order(
                  orderId: String,
                  valueWithoutTaxes: Double,
                  countryCode: String,
                  state: Option[String],
                  totalAmount: Option[Double] = None
                ){
  def withTotalAmount(amount: Double): Order = this.copy(totalAmount = Some(amount))
}