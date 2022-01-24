package etlflow.steps.aws

import etlflow.etlsteps.{S3PutStep, S3SensorStep}
import zio.ZIO
import zio.test._
import zio.test.Assertion._
import scala.concurrent.duration._

object AWSStepsTestSuite extends DefaultRunnableSpec with AwsTestHelper {

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow")(
      suite("S3 Steps")(
        testM("Execute S3Put step") {
          val step: S3PutStep = S3PutStep(
            name = "S3PutStep",
            bucket = s3_bucket,
            key = "temp/ratings.parquet",
            file = file,
            region = s3_region
          )
          assertM(step.process.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
        },
        testM("Execute S3Sensor step") {
          val step: S3SensorStep = S3SensorStep(
            name = "S3KeySensorStep",
            bucket = s3_bucket,
            prefix = "temp",
            key = "ratings.parquet",
            retry = 10,
            spaced = 5.second,
            region = s3_region
          )
          assertM(step.process.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
        }
      ) @@ TestAspect.sequential
    )
}
