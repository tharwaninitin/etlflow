package etlflow.coretests.utils

import java.text.SimpleDateFormat
import etlflow.utils.{UtilityFunctions => UF}
import com.github.t3hnar.bcrypt._
import etlflow.common.DateTimeFunctions.getTimeDifferenceAsString
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class UtilityFunctionsTestSuite extends AnyFlatSpec with should.Matchers {

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

  val actualPassword = UF.encryptKey("password")

  "GetTimeDifferenceAsString should  " should "run successfully for days" in {
    assert(actualDaysOutput == expectedDaysOutput)
  }

  "GetTimeDifferenceAsString1 should " should "run successfully for mins" in {
    assert(actualOutputMins == expectedOutputMins)
  }

  "GetTimeDifferenceAsString2 should " should "run successfully for hrs" in {
    assert(actualOutputHrs == expectedOutputHrs)
  }

  "StringFormatter should " should "return formatted string when given string with single space" in {
    assert(UF.stringFormatter(actualStepName1) == expectedStepName1)
  }

  "StringFormatter should " should "return formatted string when given string with multiple consecutive spaces" in {
    assert(UF.stringFormatter(actualStepName2) == expectedStepName2)
  }

  "StringFormatter should " should "return formatted string when given string with more than 50 characters" in {
    assert(UF.stringFormatter(actualStepName3) == expectedStepName3)
  }

  "StringFormatter should " should "return formatted string when given string with multiple consecutive underscores " in {
    assert(UF.stringFormatter(actualStepName4) == expectedStepName4)
  }

  "StringFormatter should " should "return formatted string when given string with special characters" in {
    assert(UF.stringFormatter(actualStepName5) == expectedStepName5)
  }

  "EncryptKey" should "run successfully for same password" in {
    assert("password".isBcryptedBounded(actualPassword) == true)
  }

  "EncryptKey " should " return false if wrong password provided" in {
    assert("password123".isBcryptedBounded(actualPassword) == false)
  }

}
