//package etlflow.utils
//
//import etlflow.api.Schema.{CacheDetails, CacheInfo, QueueDetails}
//import etlflow.json.{JsonApi, JsonEnv}
//import etlflow.utils.DateTimeApi.getCurrentTimestampAsString
//import io.circe.generic.semiauto.deriveEncoder
//import zio.cache.{Cache, Lookup}
//import zio.duration.Duration
//import zio.{IO, RIO, Task, UIO, URIO, ZIO}
//
//import scala.reflect.runtime.universe.{TypeTag, typeOf}
//
//object ZioCacheHelper  {
//
//  implicit val JdbcEncoder = deriveEncoder[CacheInfo]
//
//  def queueDetails(value: String, props_json: String, submitted_from: String ) : QueueDetails ={
//    QueueDetails(value, props_json, submitted_from, getCurrentTimestampAsString())
//  }
//
//  def lookUpGenerator[T: TypeTag]: String => UIO[T] =
//    typeOf[T] match {
//      case t if t =:= typeOf[QueueDetails] =>
//        value => ZIO.succeed(QueueDetails(value, "props_json", "submitted_from", "getCurrentTimestampAsString()").asInstanceOf[T])
//      case t if t =:= typeOf[String] =>
//        value => ZIO.succeed(value.asInstanceOf[T])
//    }
//
//  def createCache[T: TypeTag]: URIO[Any, Cache[String, Throwable, T]] = {
//    Cache.make(1000, Duration.Infinity, Lookup(lookUpGenerator[T]))
//  }
//
//  def getKey[T: TypeTag](cache: Cache[String, Throwable, T], key: String): IO[Throwable, T] = cache.get(key)
//
//  def removeKey[T](cache: Cache[String, Throwable, T], key: String): UIO[Unit] = cache.invalidate(key)
//
//  def toMap[T](cache: Cache[String, Throwable, T]): Map[String, String] = {
//    Map.empty
//  }
//
//  def getValues[T](cache: Cache[String, Throwable, T]): List[T] = {
//    List.empty
//  }
//
//  def keyPresent[T](cache: Cache[String, Throwable, T], key:String): Task[Boolean] =
//     cache.contains(key)
//
//  def getCacheStats[T](cache: Cache[String, Throwable, T], name: String): RIO[JsonEnv,CacheDetails] =  {
//    val data:Map[String,String] = ZioCacheHelper.toMap(cache)
//    for {
//      cacheStats <- cache.cacheStats
//      cacheSize  <- cache.size
//      cacheInfo  = CacheInfo(name, cacheStats.hits, 0, cacheSize, cacheStats.misses, 0, 0, data)
//      cacheJson <- JsonApi.convertToMap(cacheInfo,List("data"))
//      cacheDetails = CacheDetails(name,cacheJson.mapValues(x => x.toString))
//    } yield cacheDetails
//
//  }
//}