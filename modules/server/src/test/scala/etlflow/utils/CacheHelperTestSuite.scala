package etlflow.utils

import etlflow.ServerSuiteHelper
import etlflow.api.Schema.CacheDetails
import zio.test.Assertion.equalTo
import zio.test.{DefaultRunnableSpec, _}

import scala.concurrent.duration._

object CacheHelperTestSuite extends DefaultRunnableSpec with ServerSuiteHelper {

  val cache = CacheHelper.createCache[String]
  CacheHelper.putKey(cache,"key1","123")
  CacheHelper.putKey(cache,"key3","123",ttl = Some(2.second))

  val cache1 = CacheHelper.createCache[String]
  CacheHelper.putKey(cache1,"key1","123")
  CacheHelper.putKey(cache1,"key2","123")
  CacheHelper.putKey(cache1,"key3","123")
  CacheHelper.putKey(cache1,"key4","123")

  val cacheStats = for {
    login     <- CacheHelper.getCacheStats(cache1, "Login")
  } yield List(login)

  val cacheStatsOp = List(CacheDetails("Login",Map("name" -> "Login", "size" -> "4", "missCount" -> "0", "hitCount" -> "0", "requestCount" -> "0", "missRate" -> "0.0", "hitRate" -> "1.0")))

  override def spec: ZSpec[environment.TestEnvironment, Any] =
  suite("CacheHelper")(
    test("The value stored in the underlying cache should return correctly") {
      assert(CacheHelper.getKey(cache,"key1").getOrElse("NA"))(equalTo("123"))
    },
    test("The value stored in the underlying cache should return correctly") {
      assert(CacheHelper.getKey(cache,"key2"))(equalTo(None))
    },
    test("The value stored in the underlying cache should return correctly") {
      Thread.sleep(5000)
      assert(CacheHelper.getKey(cache,"key3"))(equalTo(None))
    },
    test("ToMap should return the size of underlying cache") {
      assert(CacheHelper.toMap(cache).size)(equalTo(2))
    },
    testM("Should execute the getCacheStats method") {
      assertM(cacheStats.map(x => x.map(y => y.name)))(equalTo(List("Login")))
    }
  ).provideCustomLayerShared(testJsonLayer.orDie)
}
