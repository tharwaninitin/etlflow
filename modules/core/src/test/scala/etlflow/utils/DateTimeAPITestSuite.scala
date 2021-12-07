package etlflow.utils

import etlflow.utils.DateTimeApi._
import zio.test.Assertion.equalTo
import zio.test._
import java.text.SimpleDateFormat

object DateTimeAPITestSuite {

  val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm")
  val endTimeDays = sdf.parse("2020-08-21T23:15").getTime
  val startTimeDays = sdf.parse("2020-08-18T23:15").getTime

  val actualDaysOutput = getTimeDifferenceAsString(startTimeDays, endTimeDays)
  val expectedDaysOutput = "3 days 0.0 hrs"

  val endTimeMins = sdf.parse("2020-08-19T00:30").getTime
  val startTimeMins = sdf.parse("2020-08-19T00:16").getTime

  val actualOutputMins = getTimeDifferenceAsString(startTimeMins, endTimeMins)
  val expectedOutputMins = "14.0 mins"

  val endTimeHrs = sdf.parse("2020-08-19T05:00").getTime
  val startTimeHrs = sdf.parse("2020-08-19T00:34").getTime

  val actualOutputHrs = getTimeDifferenceAsString(startTimeHrs, endTimeHrs)
  val expectedOutputHrs = "4.43 hrs"

  val spec: ZSpec[environment.TestEnvironment, Any] = {
    suite("DateTime Api")(
      test("GetTimeDifferenceAsString should  should run successfully for days") {
        assert(actualDaysOutput)(equalTo(expectedDaysOutput))
      },
      test("GetTimeDifferenceAsString1 should  should run successfully for mins") {
        assert(actualOutputMins)(equalTo(expectedOutputMins))
      },
      test("GetTimeDifferenceAsString2 should should run successfully for hrs") {
        assert(actualOutputHrs)(equalTo(expectedOutputHrs))
      }
    )
  }
}
