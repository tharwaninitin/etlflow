package etlflow.utils

import com.github.benmanes.caffeine.cache.{Caffeine, Cache => CCache}
import etlflow.api.Schema.{CacheDetails, CacheInfo}
import etlflow.json.{JsonApi, JsonEnv}
import io.circe.generic.auto._
import scalacache.caffeine._
import scalacache.modes.sync._
import scalacache.{Cache, Entry, Id}
import zio.RIO
import scala.collection.JavaConverters._
import scala.concurrent.duration._

private [etlflow] object CacheHelper {

  var default_ttl: FiniteDuration = (24 * 60).minutes

  def createCache[T]:CaffeineCache[T] = {
    val caffeineCache: CCache[String, Entry[T]] =
      Caffeine.newBuilder()
        .recordStats()
        .maximumSize(1000L)
        .build[String, Entry[T]]

    CaffeineCache(caffeineCache)
  }

  def getKey[T](cache: Cache[T], key: String): Id[Option[T]] = cache.get(key)

  def removeKey[T](cache: Cache[T], key: String): Id[Any] = cache.remove(key)

  def putKey[T](cache: Cache[T], key: String, value: T, ttl: Option[Duration] = None): Unit = cache.put(key)(value, ttl)

  def toMap[T](cache: CaffeineCache[T]): Map[String, String] = {
    cache.underlying.asMap().asScala.map(x => (x._1,x._2.toString)).toMap
  }

  def getValues[T](cache: CaffeineCache[T]): List[T] = {
    cache.underlying.asMap().asScala.values.map(_.value).toList
  }

  def getCacheStats[T](cache: CaffeineCache[T], name: String): RIO[JsonEnv,CacheDetails] = {
    val data:Map[String,String] = CacheHelper.toMap(cache)
    val cacheInfo = CacheInfo(name,
      cache.underlying.stats.hitCount(),
      cache.underlying.stats.hitRate(),
      cache.underlying.asMap().size(),
      cache.underlying.stats.missCount(),
      cache.underlying.stats.missRate(),
      cache.underlying.stats.requestCount(),
      data
    )

    for {
      cacheJson <- JsonApi.convertToMap(cacheInfo,List("data"))
      cacheDetails = CacheDetails(name,cacheJson.map(x => (x._1, x._2.toString)))
    } yield cacheDetails
  }
}
