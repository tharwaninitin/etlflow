//package etlflow.utils
//
//import zio.test.DefaultRunnableSpec
//import zio.test._
//import zio.test.Assertion.equalTo
//import scala.concurrent.duration._
//
//object CacheHelperTestSuite extends DefaultRunnableSpec {
//
//  val cache = CacheHelper.createCache[String]
//  CacheHelper.putKey(cache,"key1","123")
//  CacheHelper.putKey(cache,"key3","123",ttl = Some(2.second))
//
//  override def spec: ZSpec[environment.TestEnvironment, Any] =
//  suite("CacheHelper")(
//    test("The value stored in the underlying cache should return correctly") {
//      assert(CacheHelper.getKey(cache,"key1").getOrElse("NA"))(equalTo("123"))
//    },
//    test("The value stored in the underlying cache should return correctly") {
//      assert(CacheHelper.getKey(cache,"key2"))(equalTo(None))
//    },
//    test("The value stored in the underlying cache should return correctly") {
//      Thread.sleep(5000)
//      assert(CacheHelper.getKey(cache,"key3"))(equalTo(None))
//    },
//  )
//}
