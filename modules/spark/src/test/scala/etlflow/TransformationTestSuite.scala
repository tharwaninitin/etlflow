package etlflow

import etlflow.spark.SparkUDF
import zio.test.Assertion.{contains, equalTo}
import zio.test.{DefaultRunnableSpec, ZSpec, assert}

object TransformationTestSuite extends DefaultRunnableSpec with SparkUDF {
  override def spec: ZSpec[zio.test.environment.TestEnvironment, Any] = {
    suite("Transformation Test Suite")(
      test("Case 1 : get_24hr_formatted_from_12hr ") {
        assert(get_24hr_formatted_from_12hr("01:39:40 PM").toList)(contains("13:39:40"))
      },
      test("Case 2 : get_24hr_formatted_from_12hr ") {
        assert(get_24hr_formatted_from_12hr("11:39:40 AM").toList)(contains("11:39:40"))
      },
      test("Case 3 : get_24hr_formatted_from_12hr ") {
        assert(get_24hr_formatted_from_12hr("12:12:12 AM").toList)(contains("00:12:12"))
      },
      test("Case 4 : get_24hr_formatted_from_12hr ") {
        assert(get_24hr_formatted_from_12hr("12:12:12 PM").toList)(contains("12:12:12"))
      },
      test("Case 5 : get_24hr_formatted_from_12hr ") {
        assert(get_24hr_formatted_from_12hr("12:12:12 PM").toList)(contains("10:12:12"))
      },
      test("Case 6 : get_24hr_formatted_from_12hr ") {
        assert(get_24hr_formatted_from_12hr(null))(equalTo(None))
      },
      test("Case 1 : get_24hr_formatted ") {
        assert(get_24hr_formatted("111214").toList)(contains("11:12:14"))
      },
      test("Case 2 : get_24hr_formatted ") {
        assert(get_24hr_formatted("181920").toList)(contains("18:19:20"))
      },
      test("Case 3 : get_24hr_formatted ") {
        assert(get_24hr_formatted("121111").toList)(contains("12:11:11"))
      },
      test("Case 4 : get_24hr_formatted ") {
        assert(get_24hr_formatted("121111").toList)(contains("10:11:11"))
      },
      test("Case 5 : get_24hr_formatted ") {
        assert(get_24hr_formatted(null))(equalTo(None))
      }
    )
  }
}
