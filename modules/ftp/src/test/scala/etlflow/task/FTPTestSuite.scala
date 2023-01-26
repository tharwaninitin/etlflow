package etlflow.task

import etlflow.log.ApplicationLogger
import zio.ftp.SFtp
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test._
import zio.{Chunk, Scope, ULayer, ZIO}

object FTPTestSuite extends ZIOSpecDefault with ApplicationLogger {

  override val bootstrap: ULayer[TestEnvironment] = testEnvironment ++ zioSlf4jLogger

  def spec: Spec[TestEnvironment, Any] = (suite("FTPTestSuite")(
    test("Execute File Upload") {
      val data = ZStream.fromChunks(Chunk.fromArray("Hello World".getBytes))
      val task = SFtp.upload("/sample.txt", data).provideSome[SFtp](Scope.default)
      assertZIO(task.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    },
    test("Execute LS") {
      val task = SFtp.lsDescendant("/").runCollect.map(ch => ch.foreach(f => logger.info(f.toString)))
      assertZIO(task.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    },
    test("Execute FTPDeleteFileTask") {
      val task = FTPDeleteFileTask(
        name = "DeleteFile",
        path = "/sample.txt"
      )
      assertZIO(task.toZIO.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    }
  ) @@ TestAspect.sequential)
    .provideShared(etlflow.ftp.live("localhost", "foo", port = 2222, password = Some("foo")), etlflow.audit.noop)
}
