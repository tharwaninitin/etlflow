package etlflow

import com.github.benmanes.caffeine.cache.{Cache => CCache, Caffeine}
import zio.Has
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}

package object cache {
  type CacheEnv = Has[CacheApi.Service]
  case class CacheDetails(name: String, details: Map[String,String])
  case class CacheInfo(name: String, hitCount:Long, hitRate:Double, size:Long, missCount:Long, missRate:Double, requestCount:Long, data: Map[String,String])
  case class Entry[T](value: T, ttl: Option[Duration])
  case class Cache[T](underlying: CCache[String, Entry[T]])
  val default_ttl: FiniteDuration = (24 * 60).minutes
}
