package etlflow.steps

import etlflow.TestSuiteHelper
import etlflow.etlsteps.S3SensorStep
import org.scalatest.{FlatSpec, Matchers}
import software.amazon.awssdk.regions.Region
import scala.concurrent.duration._

class S3SensorStepTestSuite extends FlatSpec with Matchers with TestSuiteHelper {

  val step1: S3SensorStep = S3SensorStep(
    name    = "S3KeySensor",
    bucket  = "bucket",
    prefix  = "prefix",
    key     = "file.txt",
    retry   = 2,
    spaced  = 1.second,
    region  = Region.AP_SOUTH_1
  )

  runtime.unsafeRun(step1.process())
}


