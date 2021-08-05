package etlflow.cache

import zio.ZLayer
import zio.blocking.Blocking
import zio.test.Assertion.equalTo
import zio.test.{DefaultRunnableSpec, _}
import scala.concurrent.duration._

object CacheTestSuite extends DefaultRunnableSpec {

  val testCacheLayer: ZLayer[Blocking, Throwable, CacheEnv] = Implementation.live

  val cache = for {
   cache  <-  CacheApi.createCache[String]
   cache1 <-  CacheApi.createCache[String]
   _      <-  CacheApi.put(cache,"key1","123")
   _      <-  CacheApi.put(cache,"key3","123", ttl = Some(2.second))
   _      <-  CacheApi.put(cache1,"key1","123")
   _      <-  CacheApi.put(cache1,"key2","123")
   _      <-  CacheApi.put(cache1,"key3","123")
   _      <-  CacheApi.put(cache1,"key4","123")
  } yield(cache,cache1)


  val cacheStats = for {
    cache    <-  cache
    login     <- CacheApi.getStats(cache._2, "Login")
  } yield List(login)

  val cacheStatsOp = List(CacheDetails("Login",Map("name" -> "Login", "size" -> "4", "missCount" -> "0", "hitCount" -> "0", "requestCount" -> "0", "missRate" -> "0.0", "hitRate" -> "1.0")))

  override def spec: ZSpec[environment.TestEnvironment, Any] =
  suite("CacheHelper")(
    testM("The value stored in the underlying cache should return correctly - case 1 ") {
      val op = for{
        cache <-  cache
        op    <- CacheApi.get(cache._1,"key1").map(x => x.getOrElse("NA"))
      } yield op
      assertM(op)(equalTo("123"))
    },
    testM("The value stored in the underlying cache should return correctly  - case 2") {
      val op = for{
        cache <-  cache
        op    <- CacheApi.get(cache._1,"key2")
      } yield op
      assertM(op)(equalTo(None))
    },
    testM("The value stored in the underlying cache should return correctly  - case 3") {
      val op = for{
        cache <-  cache
        _     = Thread.sleep(5000)
        op    <- CacheApi.get(cache._1,"key3")
      } yield op
      assertM(op)(equalTo(None))
    },
    testM("ToMap should return the size of underlying cache  - case 4  ") {
      val size = for{
        cache <-  cache
        op    <- CacheApi.toMap(cache._1)
        size  = op.size
      } yield size
      assertM(size)(equalTo(2))
    },
    testM("Should execute the getCacheStats method  - case 5") {
      assertM(cacheStats.map(x => x.map(y => y.name)))(equalTo(List("Login")))
    }
  ).provideCustomLayerShared((testCacheLayer).orDie)
}
