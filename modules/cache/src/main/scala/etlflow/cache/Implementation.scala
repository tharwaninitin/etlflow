package etlflow.cache

import com.github.benmanes.caffeine.cache.{Caffeine, Expiry, Cache => CCache}
import zio.{Task, UIO, ULayer, ZLayer}
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.util.Try

object Implementation {

  lazy val live: ULayer[CacheEnv] = ZLayer.succeed(

    new CacheApi.Service {

      override def createCache[T]: Task[Cache[T]] = Task{

        val expirationPolicy: Expiry[String, Entry[T]] = new Expiry[String, Entry[T]]() {
          override def expireAfterCreate(key: String, value: Entry[T], currentTime: Long): Long = value.ttl.map(_.toNanos).getOrElse(Long.MaxValue)
          override def expireAfterUpdate(key: String, value: Entry[T], currentTime: Long, currentDuration: Long): Long = currentDuration
          override def expireAfterRead(key: String, value: Entry[T], currentTime: Long, currentDuration: Long): Long = currentDuration
        }

        val cache: CCache[String, Entry[T]] =
          Caffeine.newBuilder()
            .asInstanceOf[Caffeine[String, Entry[T]]]
            .recordStats()
            .expireAfter(expirationPolicy)
            .maximumSize(1000L)
            .build[String, Entry[T]]()
        Cache[T](cache)
      }

      override def get[T](cache: Cache[T], key: String): UIO[Option[T]] = UIO(Try(cache.underlying.getIfPresent(key).value).toOption)

      override def remove[T](cache: Cache[T], key: String): Task[Unit] = Task{cache.underlying.invalidate(key)}

      override def put[T](cache: Cache[T], key: String, value: T, ttl: Option[Duration]): Task[Unit] =
        Task{cache.underlying.put(key, Entry(value, ttl))}

      override def toMap[T](cache: Cache[T]): Task[Map[String, String]] = Task{
        cache.underlying.asMap().asScala.map(x => (x._1,x._2.value.toString)).toMap
      }

      override def getValues[T](cache: Cache[T]): Task[List[T]] = Task{
        cache.underlying.asMap().values.asScala.map(_.value).toList
      }

      override def getStats[T](cache: Cache[T], name: String): Task[CacheDetails] = {
        for {
          data <- toMap(cache)
          cacheMap = Map(
            "hitCount" -> cache.underlying.stats.hitCount().toString,
            "hitRate" -> cache.underlying.stats.hitRate().toString,
            "size" -> cache.underlying.asMap().size.toString,
            "missCount" -> cache.underlying.stats.missCount().toString,
            "missRate" -> cache.underlying.stats.missRate().toString,
            "requestCount" -> cache.underlying.stats.requestCount().toString,
          )
          cacheDetails = CacheDetails(name,cacheMap)
        } yield cacheDetails
      }
    }
  )
}
