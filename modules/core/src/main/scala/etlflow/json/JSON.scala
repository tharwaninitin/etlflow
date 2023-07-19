package etlflow.json

import etlflow.model.EtlFlowException.JsonDecodeException
import zio._
import zio.json._

object JSON {

  /** Converts a JSON string to an object of type `T`.
    *
    * @param str
    *   The JSON string to convert.
    * @param decoder
    *   The JSON decoder for type `T`.
    * @tparam T
    *   The type of the object.
    * @return
    *   Either an error message or the converted object.
    */
  def convertToObject[T](str: String)(implicit decoder: JsonDecoder[T]): Either[String, T] =
    if (str == null) Left("Cannot decode null value to object") else str.fromJson[T]

  /** Converts an object of type `T` to a JSON string.
    *
    * @param obj
    *   The object to convert.
    * @param encoder
    *   The JSON encoder for type `T`.
    * @tparam T
    *   The type of the object.
    * @return
    *   The JSON string representation of the object.
    */
  def convertToString[T](obj: T)(implicit encoder: JsonEncoder[T]): String = obj.toJson

  /** Converts an object of type `T` to a formatted JSON string.
    *
    * @param obj
    *   The object to convert.
    * @param encoder
    *   The JSON encoder for type `T`.
    * @tparam T
    *   The type of the object.
    * @return
    *   The formatted JSON string representation of the object.
    */
  def convertToStringPretty[T](obj: T)(implicit encoder: JsonEncoder[T]): String = obj.toJsonPretty

  /** Converts a JSON string to an object of type `T` in a ZIO effect.
    *
    * @param str
    *   The JSON string to convert.
    * @param decoder
    *   The JSON decoder for type `T`.
    * @tparam T
    *   The type of the object.
    * @return
    *   The converted object wrapped in a ZIO effect, or an error if decoding fails.
    */
  def convertToObjectZIO[T](str: String)(implicit decoder: JsonDecoder[T]): IO[JsonDecodeException, T] =
    ZIO.fromEither(convertToObject[T](str)).mapError(str => JsonDecodeException(str))

  /** Converts an object of type `T` to a JSON string in a ZIO effect.
    *
    * @param obj
    *   The object to convert.
    * @param encoder
    *   The JSON encoder for type `T`.
    * @tparam T
    *   The type of the object.
    * @return
    *   The JSON string representation of the object wrapped in a ZIO effect.
    */
  def convertToStringZIO[T](obj: T)(implicit encoder: JsonEncoder[T]): UIO[String] = ZIO.succeed(convertToString[T](obj))
}
