package etlflow.json

import etlflow.model.EtlFlowException.JsonDecodeException
import zio._
import zio.json._

trait JSON {
  def convertToObject[T](str: String)(implicit decoder: JsonDecoder[T]): IO[JsonDecodeException, T]
  def convertToString[A](obj: A)(implicit encoder: JsonEncoder[A]): UIO[String]
}

object JSON {
  def convertToObjectM[T](str: String)(implicit decoder: JsonDecoder[T]): ZIO[JSON, JsonDecodeException, T] =
    ZIO.environmentWithZIO(_.get.convertToObject[T](str))
  def convertToStringM[T](obj: T)(implicit encoder: JsonEncoder[T]): URIO[JSON, String] =
    ZIO.environmentWithZIO(_.get.convertToString[T](obj))

  def convertToObject[T](str: String)(implicit decoder: JsonDecoder[T]): Either[String, T] = str.fromJson[T]

  def convertToString[T](obj: T)(implicit decoder: JsonEncoder[T]): String       = obj.toJson
  def convertToStringPretty[T](obj: T)(implicit decoder: JsonEncoder[T]): String = obj.toJsonPretty

  private[json] val jsonImpl: JSON = new JSON {
    override def convertToObject[T](str: String)(implicit decoder: JsonDecoder[T]): IO[JsonDecodeException, T] =
      ZIO.fromEither(str.fromJson[T]).mapError(str => JsonDecodeException(str))
    override def convertToString[T](obj: T)(implicit encoder: JsonEncoder[T]): UIO[String] = ZIO.succeed(obj.toJson)
  }
}
