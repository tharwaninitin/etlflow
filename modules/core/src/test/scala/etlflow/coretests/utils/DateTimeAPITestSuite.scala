package etlflow.coretests.utils

import etlflow.coretests.TestSuiteHelper
import etlflow.utils.DateTimeApi.getTimeDifferenceAsString
import etlflow.utils.{ReflectAPI => RF}
import zio.test.Assertion.equalTo
import zio.test.{DefaultRunnableSpec, ZSpec, environment, _}

import java.text.SimpleDateFormat

object DateTimeAPITestSuite extends DefaultRunnableSpec with TestSuiteHelper {

  val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm")
  val endTimeDays = sdf.parse("2020-08-21T23:15").getTime
  val startTimeDays = sdf.parse("2020-08-18T23:15").getTime

  val actualDaysOutput = getTimeDifferenceAsString(startTimeDays,endTimeDays)
  val expectedDaysOutput = "3 days 0.0 hrs"

  val endTimeMins = sdf.parse("2020-08-19T00:30").getTime
  val startTimeMins = sdf.parse("2020-08-19T00:16").getTime

  val actualOutputMins = getTimeDifferenceAsString(startTimeMins,endTimeMins)
  val expectedOutputMins = "14.0 mins"

  val endTimeHrs = sdf.parse("2020-08-19T05:00").getTime
  val startTimeHrs = sdf.parse("2020-08-19T00:34").getTime

  val actualOutputHrs = getTimeDifferenceAsString(startTimeHrs,endTimeHrs)
  val expectedOutputHrs  = "4.43 hrs"

  val actualStepName1 = "DataTransfer For EtlJobPricing Raw Data"
  val expectedStepName1 = "datatransfer_for_etljobpricing_raw_data"

  val actualStepName2 = "DataTransfer       For EtlJobPricing Raw Data"
  val expectedStepName2 = "datatransfer_for_etljobpricing_raw_data"

  val actualStepName3 = "DataTransfer       For EtlJobPricing Raw Data_Week_Calculation"
  val expectedStepName3 = "datatransfer_for_etljobpricing_raw_data_week"

  val actualStepName4 = "DataTransfer______For EtlJobPricing Raw Data_Week_Calculation"
  val expectedStepName4 = "datatransfer_for_etljobpricing_raw_data_week_"

  val actualStepName5 = "etljobspotratingsorctobq_year_2020_week_43#&* step"
  val expectedStepName5 = "etljobspotratingsorctobq_year_2020_week_43_step"

  def spec: ZSpec[environment.TestEnvironment, Any] = {
    suite("DateTime Api Test Cases")(
      test("GetTimeDifferenceAsString should  should run successfully for days") {
        assert(actualDaysOutput)(equalTo(expectedDaysOutput))
      },
      test("GetTimeDifferenceAsString1 should  should run successfully for mins") {
        assert(actualOutputMins)(equalTo(expectedOutputMins))
      },
      test("GetTimeDifferenceAsString2 should should run successfully for hrs") {
        assert(actualOutputHrs)(equalTo(expectedOutputHrs))
      },
      test("StringFormatter should should return formatted string when given string with single space") {
        assert(RF.stringFormatter(actualStepName1) )(equalTo(expectedStepName1))
      },
      test("StringFormatter should should return formatted string when given string with multiple consecutive spaces") {
        assert(RF.stringFormatter(actualStepName2))(equalTo(expectedStepName2))
      },
      test("StringFormatter should should return formatted string when given string with more than 50 characters") {
        assert(RF.stringFormatter(actualStepName3))(equalTo(expectedStepName3))
      },
      test("StringFormatter should should return formatted string when given string with multiple consecutive underscores") {
        assert(RF.stringFormatter(actualStepName4))(equalTo(expectedStepName4))
      },
      test("StringFormatter should should return formatted string when given string with special characters") {
        assert(RF.stringFormatter(actualStepName5))(equalTo(expectedStepName5))
      }
    )
  }
}
