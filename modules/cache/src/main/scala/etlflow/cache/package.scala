package etlflow
import zio.Has

import scala.concurrent.duration.{DurationInt, FiniteDuration}

package object cache {
  type CacheEnv = Has[CacheApi.Service]
  case class CacheDetails(name:String,details:Map[String,String])
  case class CacheInfo(name:String,hitCount:Long,hitRate:Double,size:Long,missCount:Long,missRate:Double,requestCount:Long,data: Map[String,String])
  var default_ttl: FiniteDuration = (24 * 60).minutes

}
