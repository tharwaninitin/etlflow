package etlflow.utils

import java.text.SimpleDateFormat

import etlflow.utils.{UtilityFunctions => UF}
import org.scalatest.{FlatSpec, Matchers}

class UtilityFunctionsTestSuite extends FlatSpec with Matchers {

  val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm")
  val endTimeDays = sdf.parse("2020-08-21T23:15").getTime
  val startTimeDays = sdf.parse("2020-08-18T23:15").getTime

  val actualDaysOutput = UF.getTimeDifferenceAsString(startTimeDays,endTimeDays)
  val expectedDaysOutput = "3 days 0.0 hrs"

  val endTimeMins = sdf.parse("2020-08-19T00:30").getTime
  val startTimeMins = sdf.parse("2020-08-19T00:16").getTime

  val actualOutputMins = UF.getTimeDifferenceAsString(startTimeMins,endTimeMins)
  val expectedOutputMins = "14.0 mins"

  val endTimeHrs = sdf.parse("2020-08-19T05:00").getTime
  val startTimeHrs = sdf.parse("2020-08-19T00:34").getTime

  val actualOutputHrs = UF.getTimeDifferenceAsString(startTimeHrs,endTimeHrs)
  val expectedOutputHrs  = "4.43 hrs"

  "GetTimeDifferenceAsString should  " should "run successfully for days" in {
    assert(actualDaysOutput == expectedDaysOutput)
  }

  "GetTimeDifferenceAsString1 should " should "run successfully for mins" in {
    assert(actualOutputMins == expectedOutputMins)
  }

  "GetTimeDifferenceAsString2 should " should "run successfully for hrs" in {
    assert(actualOutputHrs == expectedOutputHrs)
  }
}
