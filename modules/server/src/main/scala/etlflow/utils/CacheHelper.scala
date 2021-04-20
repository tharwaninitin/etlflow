package etlflow.utils

import com.github.benmanes.caffeine.cache.{Caffeine, Cache => CCache}
import scalacache.{Cache, Entry, Id}
import scalacache.caffeine._
import scalacache.modes.sync._
import scala.concurrent.duration._
import scala.collection.JavaConverters._

object CacheHelper {

  var default_ttl = (24 * 60).minutes
  def createCache[T]:CaffeineCache[T] = {
    val caffeineCache: CCache[String, Entry[T]] =
      Caffeine.newBuilder()
        .recordStats()
        .maximumSize(1000L)
        .build[String, Entry[T]]

    CaffeineCache(caffeineCache)
  }

  def getKey[T](cache: Cache[T], key: String): Id[Option[T]] =
    cache.get(key)

  def putKey[T](cache: Cache[T], key: String, value: T, ttl: Option[Duration] = None): Unit =
    cache.put(key)(value, ttl)

  def toMap[T](cache: CaffeineCache[T]) = {
    cache.underlying.asMap().asScala.map(x => (x._1,x._2.toString)).toMap
  }
}
