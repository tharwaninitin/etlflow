package etlflow

import etlflow.aws.{S3, S3Api, S3Env}
import etlflow.etltask.{S3PutTask, S3SensorTask}
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
    S3.live(s3Region, Some(Credential.AWS("etlflow", "etlflowpass")), Some("http://localhost:9000")).orDie

  def spec: ZSpec[environment.TestEnvironment, Any] =
    (suite("S3 Steps")(
      testM("Execute createBucket step") {
        val step = S3Api.createBucket(s3Bucket).fold(_ => "ok", _ => "ok")
        assertM(step)(equalTo("ok"))
      },
      testM("Execute S3Put step") {
        val step = S3PutTask(
          name = "S3PutStep",
          bucket = s3Bucket,
          key = s3Path,
          file = localFile,
          overwrite = true
        ).executeZio
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute S3Put step overwrite false") {
        val step = S3PutTask(
          name = "S3PutStep",
          bucket = s3Bucket,
          key = s3Path,
          file = localFile,
          overwrite = false
        ).executeZio
        assertM(step.foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.fail("ok")))(
          equalTo("File at path s3://test/temp/ratings.csv already exist")
        )
      },
      testM("Execute S3Sensor step") {
        val step = S3SensorTask(
          name = "S3KeySensorStep",
          bucket = s3Bucket,
          key = s3Path,
          retry = 3,
          spaced = 5.second
        ).executeZio
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute getObject api") {
        val step = S3Api
          .getObject(s3Bucket, s3Path)
          .transduce(ZTransducer.utf8Decode)
          .transduce(ZTransducer.splitLines)
          .tap(op => UIO(logger.info(op)))
          .runCollect
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute getObject to putObject stream") {
        val in = S3Api.getObject(s3Bucket, s3Path)
        val op = S3Api.putObject(s3Bucket, "temp/ratings2.csv", in, 124)
        assertM(op.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      testM("Execute getObject api") {
        val step = S3Api
          .getObject(s3Bucket, "temp/ratings2.csv")
          .transduce(ZTransducer.utf8Decode)
          .transduce(ZTransducer.splitLines)
          .tap(op => UIO(logger.info(op)))
          .runCollect
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    ) @@ TestAspect.sequential).provideLayerShared(env ++ Clock.live ++ log.noLog)
}
