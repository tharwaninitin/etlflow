package etlflow

import etlflow.spark.SparkUDF
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class TransformationTestSuite extends AnyFlatSpec with should.Matchers with SparkUDF {

  "get_24hr_formatted_from_12hr function" should "convert 12 hr format to 24 hr format" in {
    assert(get_24hr_formatted_from_12hr("01:39:40 PM").contains("13:39:40"))
    assert(get_24hr_formatted_from_12hr("11:39:40 AM").contains("11:39:40"))
    assert(get_24hr_formatted_from_12hr("12:12:12 AM")==Some("00:12:12"))
    assert(get_24hr_formatted_from_12hr("12:12:12 PM")==Some("12:12:12"))
    assert(get_24hr_formatted_from_12hr("12:12:12 PM")!=Some("10:12:12"))
    assert(get_24hr_formatted_from_12hr(null)==None)
  }

  "get_24hr_formatted function" should "format 24 hr time properly" in {
    assert(get_24hr_formatted("111214").contains("11:12:14"))
    assert(get_24hr_formatted("181920").contains("18:19:20"))
    assert(get_24hr_formatted("121111")==Some("12:11:11"))
    assert(get_24hr_formatted("121111")!=Some("10:11:11"))
    assert(get_24hr_formatted(null)==None)
  }
}
