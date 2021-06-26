package etlflow

import io.circe.syntax._
import io.circe.{Decoder, Encoder, Json}
import zio.{Has, Task, ZIO}

package object json {

  type JsonEnv = Has[JsonApi.Service]

  def removeField(json:Json)(keys:List[String]): Json = json.withObject(obj=>keys.foldLeft(obj)((acc, s)=>acc.remove(s)).asJson)

  object JsonApi {
    trait Service {
      def convertToObject[T](str: String)(implicit Decoder: Decoder[T]): Task[T]
      def convertToJsonByRemovingKeysAsMap[A](entity: A, Keys:List[String])(implicit encoder: Encoder[A]): Task[Map[String,Any]]
      def convertToJsonByRemovingKeys[A](obj: A, Keys:List[String])(implicit encoder: Encoder[A]): Task[Json]
      def convertToJson[A](obj: A)(implicit encoder: Encoder[A]): Task[String]
      def convertToJsonJacksonByRemovingKeys(entity: AnyRef, keys: List[String]): Task[String]
      def convertToJsonJacksonByRemovingKeysAsMap(entity: AnyRef, keys: List[String]): Task[Map[String,Any]]
    }

    def convertToObject[T](str: String)(implicit Decoder: Decoder[T]): ZIO[JsonEnv, Throwable, T] =
      ZIO.accessM(_.get.convertToObject[T](str))
    def convertToJsonByRemovingKeysAsMap[T](entity: T, Keys:List[String])(implicit encoder: Encoder[T]):
      ZIO[JsonEnv, Throwable, Map[String,Any]] = ZIO.accessM(_.get.convertToJsonByRemovingKeysAsMap[T](entity, Keys))
    def convertToJsonByRemovingKeys[T](obj: T, Keys:List[String])(implicit encoder: Encoder[T]):
      ZIO[JsonEnv, Throwable, Json] = ZIO.accessM(_.get.convertToJsonByRemovingKeys[T](obj, Keys))
    def convertToJson[A](obj: A)(implicit encoder: Encoder[A]): ZIO[JsonEnv, Throwable, String] =
      ZIO.accessM(_.get.convertToJson(obj))
    def convertToJsonJacksonByRemovingKeys(entity: AnyRef, keys: List[String]): ZIO[JsonEnv, Throwable, String] =
      ZIO.accessM(_.get.convertToJsonJacksonByRemovingKeys(entity,keys))
    def convertToJsonJacksonByRemovingKeysAsMap(entity: AnyRef, keys: List[String]): ZIO[JsonEnv, Throwable, Map[String,Any]] =
      ZIO.accessM(_.get.convertToJsonJacksonByRemovingKeysAsMap(entity,keys))
  }
}
