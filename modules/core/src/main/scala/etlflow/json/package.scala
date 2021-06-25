package etlflow

import io.circe
import io.circe.syntax._
import io.circe.{Decoder, Encoder, Json}
import org.json4s.{DefaultFormats, Formats}
import org.slf4j.{Logger, LoggerFactory}
import zio.{Has, Task, ZIO}

package object json {

  val gcp_logger: Logger = LoggerFactory.getLogger(getClass.getName)
  type JsonService = Has[JsonService.Service]

  def removeField(json:Json)(keys:List[String]):Json=
    json.withObject(obj=>keys.foldLeft(obj)((acc, s)=>acc.remove(s)).asJson)

  object JsonService {
    trait Service {
      def convertToObject[T](str: String)(implicit Decoder: Decoder[T]): Task[T]
      def convertToJsonByRemovingKeysAsMap[A](entity: A, Keys:List[String])(implicit encoder: Encoder[A]): Task[Map[String,Any]]
      def convertToJsonByRemovingKeys[A](obj: A, Keys:List[String])(implicit encoder: Encoder[A]): Task[Json]
      def convertToObjectEither[T](str: String)(implicit Decoder: Decoder[T]): Task[Either[circe.Error, T]]
      def convertToJson(entity: AnyRef): Task[String]
      def convertToJsonJacksonByRemovingKeys(entity: AnyRef, keys: List[String]): Task[String]
      def convertToJsonJacksonByRemovingKeysAsMap(entity: AnyRef, keys: List[String]): Task[Map[String,Any]]
      def convertToObjectUsingJackson[T](str: String, fmt: Formats = DefaultFormats)(implicit mf: Manifest[T]): Task[T]
    }

    def convertToObject[T](str: String)(implicit Decoder: Decoder[T]): ZIO[JsonService, Throwable, T] =
      ZIO.accessM(_.get.convertToObject[T](str))
    def convertToJsonByRemovingKeysAsMap[T](entity: T, Keys:List[String])(implicit encoder: Encoder[T]):
      ZIO[JsonService, Throwable, Map[String,Any]] = ZIO.accessM(_.get.convertToJsonByRemovingKeysAsMap[T](entity, Keys))
    def convertToJsonByRemovingKeys[T](obj: T, Keys:List[String])(implicit encoder: Encoder[T]):
      ZIO[JsonService, Throwable, Json] = ZIO.accessM(_.get.convertToJsonByRemovingKeys[T](obj, Keys))
    def convertToObjectEither[T](str: String)(implicit Decoder: Decoder[T]) : ZIO[JsonService, Throwable, Either[circe.Error, T]] =
      ZIO.accessM(_.get.convertToObjectEither[T](str))
    def convertToJson(entity: AnyRef): ZIO[JsonService, Throwable, String] =
      ZIO.accessM(_.get.convertToJson(entity))
    def convertToJsonJacksonByRemovingKeys(entity: AnyRef, keys: List[String]): ZIO[JsonService, Throwable, String] =
      ZIO.accessM(_.get.convertToJsonJacksonByRemovingKeys(entity,keys))
    def convertToJsonJacksonByRemovingKeysAsMap(entity: AnyRef, keys: List[String]): ZIO[JsonService, Throwable, Map[String,Any]] =
      ZIO.accessM(_.get.convertToJsonJacksonByRemovingKeysAsMap(entity,keys))
    def convertToObjectUsingJackson[T](str: String, fmt: Formats = DefaultFormats)(implicit mf: Manifest[T]): ZIO[JsonService, Throwable, T] =
      ZIO.accessM(_.get.convertToObjectUsingJackson[T](str))
  }
}
