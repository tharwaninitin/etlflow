package etlflow.steps

import etlflow.TestSuiteHelper
import etlflow.etlsteps.{GCSPutStep, GCSSensorStep}
import zio.ZIO
import zio.test._
import zio.test.Assertion._
import scala.concurrent.duration._

object GCSStepTestSuite extends DefaultRunnableSpec with TestSuiteHelper {

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow")(
      suite("GCS Steps")(
        testM("Execute GCSPut step") {
          val step = GCSPutStep(
            name    = "S3PutStep",
            bucket  = gcs_bucket,
            key     = "temp/ratings.parquet",
            file    = file
          )
          assertM(step.process().foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
        },
        testM("Execute GCSSensor step") {
          val step = GCSSensorStep(
            name    = "GCSKeySensor",
            bucket  = gcs_bucket,
            prefix  = "temp",
            key     = "ratings.parquet",
            retry   = 10,
            spaced  = 5.second
          )
          assertM(step.process().foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
        }
      )
    )
}


