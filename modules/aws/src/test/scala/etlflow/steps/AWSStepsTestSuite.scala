package etlflow.steps

import etlflow.AwsTestHelper
import etlflow.aws.{S3, S3Env}
import etlflow.etlsteps.{S3PutStep, S3SensorStep}
import etlflow.model.Credential
import zio.clock.Clock
import zio.test.Assertion.equalTo
import zio.test._
import zio.{ULayer, ZIO}
import scala.concurrent.duration._

object AWSStepsTestSuite extends DefaultRunnableSpec with AwsTestHelper {

  val env: ULayer[S3Env] =
    S3.live(s3_region, Some(Credential.AWS("etlflow", "etlflowpass")), Some("http://localhost:9000")).orDie

  def spec: ZSpec[environment.TestEnvironment, Any] =
    (suite("S3 Steps")(
      testM("Execute S3Put step") {
        val step = S3PutStep(
          name = "S3PutStep",
          bucket = s3_bucket,
          key = "temp/ratings.parquet",
          file = file
        ).process
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute S3Sensor step") {
        val step = S3SensorStep(
          name = "S3KeySensorStep",
          bucket = s3_bucket,
          prefix = "temp",
          key = "ratings.parquet",
          retry = 10,
          spaced = 5.second
        ).process
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    ) @@ TestAspect.sequential).provideLayerShared(env ++ Clock.live)
}
