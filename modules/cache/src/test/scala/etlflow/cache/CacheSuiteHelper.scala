package etlflow.cache

import etlflow.cache
import etlflow.json.JsonEnv
import zio.ZLayer
import zio.blocking.Blocking


trait CacheSuiteHelper {
  val testJsonLayer: ZLayer[Blocking, Throwable, JsonEnv] = etlflow.json.Implementation.live
  val testCacheLayer: ZLayer[Blocking, Throwable, CacheEnv] = cache.Implementation.live
}
