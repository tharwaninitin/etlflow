package etlflow

import etlflow.spark.SparkUDF
import zio.test._

@SuppressWarnings(Array("org.wartremover.warts.Null"))
object TransformationTestSuite extends SparkUDF {
  val spec: Spec[Any, Any] =
    suite("Transformation Test Suite")(
      test("Case 1 : get_24hr_formatted_from_12hr ") {
        assertTrue(get24hrFrom12hr("01:39:40 PM").toList.contains("13:39:40"))
      },
      test("Case 2 : get_24hr_formatted_from_12hr ") {
        assertTrue(get24hrFrom12hr("11:39:40 AM").toList.contains("11:39:40"))
      },
      test("Case 3 : get_24hr_formatted_from_12hr ") {
        assertTrue(get24hrFrom12hr("12:12:12 AM").toList.contains("00:12:12"))
      },
      test("Case 4 : get_24hr_formatted_from_12hr ") {
        assertTrue(get24hrFrom12hr("12:12:12 PM").toList.contains("12:12:12"))
      },
      test("Case 5 : get_24hr_formatted_from_12hr ") {
        assertTrue(get24hrFrom12hr("12:12:12 PM").toList.contains("12:12:12"))
      },
      test("Case 6 : get_24hr_formatted_from_12hr ") {
        assertTrue(get24hrFrom12hr(null).isEmpty)
      },
      test("Case 1 : get_24hr_formatted ") {
        assertTrue(get24hr("111214").toList.contains("11:12:14"))
      },
      test("Case 2 : get_24hr_formatted ") {
        assertTrue(get24hr("181920").toList.contains("18:19:20"))
      },
      test("Case 3 : get_24hr_formatted ") {
        assertTrue(get24hr("121111").toList.contains("12:11:11"))
      },
      test("Case 4 : get_24hr_formatted ") {
        assertTrue(get24hr("121111").toList.contains("12:11:11"))
      },
      test("Case 5 : get_24hr_formatted ") {
        assertTrue(get24hr(null).isEmpty)
      }
    )
}
