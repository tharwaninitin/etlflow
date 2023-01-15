package etlflow.json

import etlflow.model.EtlFlowException.JsonDecodeException
import zio._
import zio.json._

object JSON {

  def convertToObject[T](str: String)(implicit decoder: JsonDecoder[T]): Either[String, T] = str.fromJson[T]

  def convertToString[T](obj: T)(implicit decoder: JsonEncoder[T]): String = obj.toJson

  def convertToStringPretty[T](obj: T)(implicit decoder: JsonEncoder[T]): String = obj.toJsonPretty

  def convertToObjectZIO[T](str: String)(implicit decoder: JsonDecoder[T]): IO[JsonDecodeException, T] =
    ZIO.fromEither(convertToObject[T](str)).mapError(str => JsonDecodeException(str))

  def convertToStringZIO[T](obj: T)(implicit encoder: JsonEncoder[T]): UIO[String] = ZIO.succeed(convertToString[T](obj))
}
