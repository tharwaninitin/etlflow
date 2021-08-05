package etlflow.cache

import zio.{RIO, Task, UIO, URIO, ZIO}
import scala.concurrent.duration.Duration

object CacheApi {

  trait Service {
    def createCache[T]: Task[Cache[T]]
    def get[T](cache: Cache[T], key: String): UIO[Option[T]]
    def remove[T](cache: Cache[T], key: String): Task[Unit]
    def put[T](cache: Cache[T], key: String, value: T, ttl: Option[Duration] = None): Task[Unit]
    def toMap[T](cache: Cache[T]): Task[Map[String, String]]
    def getValues[T](cache: Cache[T]): Task[List[T]]
    def getStats[T](cache: Cache[T], name: String): Task[CacheDetails]
  }

  def createCache[T]: ZIO[CacheEnv, Throwable, Cache[T]] =
    ZIO.accessM(_.get.createCache[T])
  def get[T](cache: Cache[T], key: String): URIO[CacheEnv, Option[T]] =
    ZIO.accessM(_.get.get[T](cache, key))
  def remove[T](cache: Cache[T], key: String): ZIO[CacheEnv, Throwable, Unit] =
    ZIO.accessM(_.get.remove[T](cache, key))
  def put[T](cache: Cache[T], key: String, value: T, ttl: Option[Duration] = None): ZIO[CacheEnv, Throwable, Unit] =
    ZIO.accessM(_.get.put[T](cache, key, value, ttl))
  def toMap[T](cache: Cache[T]): ZIO[CacheEnv, Throwable, Map[String, String]] =
    ZIO.accessM(_.get.toMap[T](cache))
  def getValues[T](cache: Cache[T]): ZIO[CacheEnv, Throwable, List[T]] =
    ZIO.accessM(_.get.getValues[T](cache))
  def getStats[T](cache: Cache[T], name: String): ZIO[CacheEnv, Throwable, CacheDetails] =
    ZIO.accessM(_.get.getStats[T](cache,name))
}
