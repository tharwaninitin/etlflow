package etlflow.utils

import org.scalatest.matchers.must.Matchers
import org.scalatest.flatspec.AnyFlatSpec
import scala.concurrent.duration._

class CacheHelperTestSuite  extends AnyFlatSpec with Matchers {

  val cache = CacheHelper.createCache[String]
  CacheHelper.putKey(cache,"key1","123")
  CacheHelper.putKey(cache,"key3","123",ttl = Some(2.second))


  "The value stored in the underlying cache" should "run successfully" in {
    assert(CacheHelper.getKey(cache,"key1").getOrElse("NA") == "123")
  }

  "The value stored in the underlying cache" should "return None" in {
    assert(CacheHelper.getKey(cache,"key2") == None)
  }

  "The value stored in the underlying cache" should "return None if the given key exists but the value has expired" in {
    Thread.sleep(5000)
    assert(CacheHelper.getKey(cache,"key3") == None)
  }
}
