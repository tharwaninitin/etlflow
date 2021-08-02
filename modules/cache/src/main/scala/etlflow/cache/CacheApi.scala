package etlflow.cache

import etlflow.json.JsonEnv
import scalacache.caffeine.CaffeineCache
import scalacache.{Cache, Id}
import zio.{RIO, Task, ZIO}

import scala.concurrent.duration.Duration
import scala.reflect.runtime.universe.TypeTag

object CacheApi {

  trait Service {
    def createCache[T]:Task[CaffeineCache[T]]
    def getKey[T](cache: Cache[T], key: String): Task[Id[Option[T]]]
    def removeKey[T](cache: Cache[T], key: String): Task[Id[Any]]
    def putKey[T](cache: Cache[T], key: String, value: T, ttl: Option[Duration] = None): Task[Unit]
    def toMap[T](cache: CaffeineCache[T]): Task[Map[String, String]]
    def getValues[T](cache: CaffeineCache[T]): Task[List[T]]
    def getCacheStats[T](cache: CaffeineCache[T], name: String): RIO[CacheEnv with JsonEnv,CacheDetails]
  }

  def createCache[T: TypeTag]: ZIO[CacheEnv, Throwable, CaffeineCache[T]] =
    ZIO.accessM(_.get.createCache[T])
  def getKey[T: TypeTag](cache: Cache[T], key: String): ZIO[CacheEnv, Throwable, Id[Option[T]]] =
    ZIO.accessM(_.get.getKey[T](cache, key))
  def removeKey[T: TypeTag](cache: Cache[T], key: String): ZIO[CacheEnv, Throwable, Id[Any]] =
    ZIO.accessM(_.get.removeKey[T](cache, key))
  def putKey[T: TypeTag](cache: Cache[T], key: String, value: T, ttl: Option[Duration] = None): ZIO[CacheEnv, Throwable,Unit] =
    ZIO.accessM(_.get.putKey[T](cache, key, value, ttl))
  def toMap[T: TypeTag](cache: CaffeineCache[T]): ZIO[CacheEnv, Throwable, Map[String, String]] =
    ZIO.accessM(_.get.toMap[T](cache))
  def getValues[T: TypeTag](cache: CaffeineCache[T]): ZIO[CacheEnv, Throwable, List[T]] =
    ZIO.accessM(_.get.getValues[T](cache))
  def getCacheStats[T](cache: CaffeineCache[T], name: String): RIO[CacheEnv with JsonEnv,CacheDetails] =
    ZIO.accessM(_.get.getCacheStats[T](cache,name))
}
