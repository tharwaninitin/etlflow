package etlflow.scheduler

import java.util.concurrent.TimeUnit
import com.github.benmanes.caffeine.cache.{Caffeine, Cache => CCache}
import scalacache.{Cache, Entry, Id}
import scalacache.caffeine._
import scalacache.modes.sync._
import scala.concurrent.duration._

object CacheHelper {

  def createCache[T](expireAfterWriteInMinutes: Int): Cache[T] = {
    val caffeineCache: CCache[String, Entry[T]] =
      Caffeine.newBuilder()
        .expireAfterWrite(expireAfterWriteInMinutes, TimeUnit.MINUTES)
        .maximumSize(1000L)
        .build[String, Entry[T]]

    CaffeineCache(caffeineCache)
  }

  def getKey[T](cache: Cache[T], key: String): Id[Option[T]] =
    cache.get(key)

  def putKey[T](cache: Cache[T], key: String, value: T, ttl: Option[Duration] = None): Unit =
    cache.put(key)(value, ttl)

}
