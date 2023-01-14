package etlflow

import etlflow.model.EtlFlowException.JsonDecodeException
import zio._
import zio.json._

package object json {

  trait JsonApi {
    def convertToObject[T](str: String)(implicit Decoder: JsonDecoder[T]): IO[JsonDecodeException, T]
    def convertToString[A](obj: A)(implicit encoder: JsonEncoder[A]): UIO[String]
  }

  object JsonApi {
    def convertToObjectM[T](str: String)(implicit Decoder: JsonDecoder[T]): ZIO[JsonApi, JsonDecodeException, T] =
      ZIO.environmentWithZIO(_.get.convertToObject[T](str))
    def convertToStringM[T](obj: T)(implicit encoder: JsonEncoder[T]): URIO[JsonApi, String] =
      ZIO.environmentWithZIO(_.get.convertToString[T](obj))
    def convertToObject[T](str: String)(implicit Decoder: JsonDecoder[T]): Either[String, T] = str.fromJson[T]
    def convertToString[T](obj: T)(implicit Decoder: JsonEncoder[T]): String                 = obj.toJson
    def convertToStringPretty[T](obj: T)(implicit Decoder: JsonEncoder[T]): String           = obj.toJsonPretty
  }
}
