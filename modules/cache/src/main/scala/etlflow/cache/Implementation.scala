package etlflow.cache

import com.github.benmanes.caffeine.cache.{Caffeine, Cache => CCache}
import etlflow.json.{JsonApi, JsonEnv}
import io.circe.generic.auto._
import scalacache.caffeine.CaffeineCache
import scalacache.modes.sync._
import scalacache.{Cache, Entry, Id}
import zio.{RIO, Task, ULayer, ZLayer}

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration

object Implementation {

  lazy val live: ULayer[CacheEnv] = ZLayer.succeed(
    new CacheApi.Service {
      override def createCache[T]: Task[CaffeineCache[T]] = Task{
        val caffeineCache: CCache[String, Entry[T]] =
          Caffeine.newBuilder()
            .recordStats()
            .maximumSize(1000L)
            .build[String, Entry[T]]

        CaffeineCache(caffeineCache)
      }

      override def getKey[T](cache: Cache[T], key: String): Task[Id[Option[T]]] = Task{cache.get(key)}

      override def removeKey[T](cache: Cache[T], key: String): Task[Id[Any]] = Task{cache.remove(key)}

      override def putKey[T](cache: Cache[T], key: String, value: T, ttl: Option[Duration]): Task[Unit] =
        Task{cache.put(key)(value, ttl)}

      override def toMap[T](cache: CaffeineCache[T]): Task[Map[String, String]] = Task{
        cache.underlying.asMap().asScala.map(x => (x._1,x._2.toString)).toMap
      }

      override def getValues[T](cache: CaffeineCache[T]): Task[List[T]] = Task{
        cache.underlying.asMap().asScala.values.map(_.value).toList
      }

      override def getCacheStats[T](cache: CaffeineCache[T], name: String): RIO[CacheEnv with JsonEnv, CacheDetails] = {
        for {
          data <- toMap(cache)
          cacheInfo = CacheInfo(name,
            cache.underlying.stats.hitCount(),
            cache.underlying.stats.hitRate(),
            cache.underlying.asMap().size(),
            cache.underlying.stats.missCount(),
            cache.underlying.stats.missRate(),
            cache.underlying.stats.requestCount(),
            data
          )
          cacheJson <- JsonApi.convertToMap(cacheInfo,List("data"))
          cacheDetails = CacheDetails(name,cacheJson.map(x => (x._1, x._2.toString)))
        } yield cacheDetails
      }
    }
  )
}
