package etlflow

import etlflow.aws.{S3, S3Api, S3Env}
import etlflow.etlsteps.{S3PutStep, S3SensorStep}
import etlflow.model.Credential
import etlflow.utils.ApplicationLogger
import zio.clock.Clock
import zio.stream.ZTransducer
import zio.test.Assertion.equalTo
import zio.test._
import zio.{UIO, ULayer, ZIO}
import scala.concurrent.duration._

object S3TestSuite extends DefaultRunnableSpec with TestHelper with ApplicationLogger {

  val env: ULayer[S3Env] =
    S3.live(s3_region, Some(Credential.AWS("etlflow", "etlflowpass")), Some("http://localhost:9000")).orDie

  def spec: ZSpec[environment.TestEnvironment, Any] =
    (suite("S3 Steps")(
      testM("Execute createBucket step") {
        val step = S3Api.createBucket(s3_bucket).fold(_ => "ok", _ => "ok")
        assertM(step)(equalTo("ok"))
      },
      testM("Execute S3Put step") {
        val step = S3PutStep(
          name = "S3PutStep",
          bucket = s3_bucket,
          key = s3_path,
          file = local_file,
          overwrite = true
        ).execute
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute S3Put step overwrite false") {
        val step = S3PutStep(
          name = "S3PutStep",
          bucket = s3_bucket,
          key = s3_path,
          file = local_file,
          overwrite = false
        ).execute
        assertM(step.foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.fail("ok")))(
          equalTo("File at path s3://test/temp/ratings.csv already exist")
        )
      },
      testM("Execute S3Sensor step") {
        val step = S3SensorStep(
          name = "S3KeySensorStep",
          bucket = s3_bucket,
          key = s3_path,
          retry = 3,
          spaced = 5.second
        ).execute
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute getObject api") {
        val step = S3Api
          .getObject(s3_bucket, s3_path)
          .transduce(ZTransducer.utf8Decode)
          .transduce(ZTransducer.splitLines)
          .tap(op => UIO(logger.info(op)))
          .runCollect
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute getObject to putObject stream") {
        val in = S3Api.getObject(s3_bucket, s3_path)
        val op = S3Api.putObject(s3_bucket, "temp/ratings2.csv", in, 124)
        assertM(op.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute getObject api") {
        val step = S3Api
          .getObject(s3_bucket, "temp/ratings2.csv")
          .transduce(ZTransducer.utf8Decode)
          .transduce(ZTransducer.splitLines)
          .tap(op => UIO(logger.info(op)))
          .runCollect
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    ) @@ TestAspect.sequential).provideLayerShared(env ++ Clock.live ++ log.nolog)
}