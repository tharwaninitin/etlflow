package etlflow.utils

import etlflow.utils.DateTimeApi._
import zio.test._
import java.text.SimpleDateFormat

object DateTimeAPITestSuite {

  private val sdf           = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm")
  private val endTimeDays   = sdf.parse("2020-08-21T23:15").getTime
  private val startTimeDays = sdf.parse("2020-08-18T23:15").getTime

  private val actualDaysOutput   = getTimeDifferenceAsString(startTimeDays, endTimeDays)
  private val expectedDaysOutput = "3 days 0.0 hrs"

  private val endTimeMins   = sdf.parse("2020-08-19T00:30").getTime
  private val startTimeMins = sdf.parse("2020-08-19T00:16").getTime

  private val actualOutputMins   = getTimeDifferenceAsString(startTimeMins, endTimeMins)
  private val expectedOutputMins = "14.0 mins"

  private val endTimeHrs   = sdf.parse("2020-08-19T05:00").getTime
  private val startTimeHrs = sdf.parse("2020-08-19T00:34").getTime

  private val actualOutputHrs   = getTimeDifferenceAsString(startTimeHrs, endTimeHrs)
  private val expectedOutputHrs = "4.43 hrs"

  val spec: Spec[TestEnvironment, Any] =
    suite("DateTime Api")(
      test("GetTimeDifferenceAsString should  should run successfully for days") {
        assertTrue(actualDaysOutput == expectedDaysOutput)
      },
      test("GetTimeDifferenceAsString1 should  should run successfully for mins") {
        assertTrue(actualOutputMins == expectedOutputMins)
      },
      test("GetTimeDifferenceAsString2 should should run successfully for hrs") {
        assertTrue(actualOutputHrs == expectedOutputHrs)
      }
    )
}
