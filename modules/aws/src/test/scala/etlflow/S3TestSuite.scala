package etlflow

import etlflow.aws.{S3, S3Api, S3Env}
import etlflow.model.Credential
import etlflow.task.{S3PutTask, S3SensorTask}
import etlflow.utils.ApplicationLogger
import zio.Clock
import zio.stream.ZTransducer
import zio.test.Assertion.equalTo
import zio.test._
import zio.{UIO, ULayer, ZIO}
import scala.concurrent.duration._
import zio.test.ZIOSpecDefault

object S3TestSuite extends ZIOSpecDefault with TestHelper with ApplicationLogger {

  val env: ULayer[S3Env] =
    S3.live(s3Region, Some(Credential.AWS("etlflow", "etlflowpass")), Some("http://localhost:9000")).orDie

  def spec: ZSpec[environment.TestEnvironment, Any] =
    (suite("S3 Tasks")(
      test("Execute createBucket task") {
        val task = S3Api.createBucket(s3Bucket).fold(_ => "ok", _ => "ok")
        assertM(task)(equalTo("ok"))
      },
      test("Execute S3Put task") {
        val task = S3PutTask(
          name = "S3PutTask",
          bucket = s3Bucket,
          key = s3Path,
          file = localFile,
          overwrite = true
        ).execute
        assertM(task.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      test("Execute S3Put task overwrite false") {
        val task = S3PutTask(
          name = "S3PutTask",
          bucket = s3Bucket,
          key = s3Path,
          file = localFile,
          overwrite = false
        ).execute
        assertM(task.foldZIO(ex => ZIO.succeed(ex.getMessage), _ => ZIO.fail("ok")))(
          equalTo("File at path s3://test/temp/ratings.csv already exist")
        )
      },
      test("Execute S3Sensor task") {
        val task = S3SensorTask(
          name = "S3KeySensorTask",
          bucket = s3Bucket,
          key = s3Path,
          retry = 3,
          spaced = 5.second
        ).execute
        assertM(task.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      test("Execute getObject api") {
        val task = S3Api
          .getObject(s3Bucket, s3Path)
          .transduce(ZTransducer.utf8Decode)
          .transduce(ZTransducer.splitLines)
          .tap(op => ZIO.succeed(logger.info(op)))
          .runCollect
        assertM(task.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      test("Execute getObject to putObject stream") {
        val in = S3Api.getObject(s3Bucket, s3Path)
        val op = S3Api.putObject(s3Bucket, "temp/ratings2.csv", in, 124)
        assertM(op.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      test("Execute getObject api") {
        val task = S3Api
          .getObject(s3Bucket, "temp/ratings2.csv")
          .transduce(ZTransducer.utf8Decode)
          .transduce(ZTransducer.splitLines)
          .tap(op => ZIO.succeed(logger.info(op)))
          .runCollect
        assertM(task.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    ) @@ TestAspect.sequential).provideLayerShared(env ++ Clock.live ++ log.noLog)
}
