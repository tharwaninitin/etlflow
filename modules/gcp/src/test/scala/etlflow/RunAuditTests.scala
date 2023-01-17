package etlflow

import etlflow.audit.CreateBQ.program
import etlflow.log.ApplicationLogger
import gcp4zio.bq.BQ
import zio.test.Assertion.equalTo
import zio.test._
import zio.{ULayer, ZIO}

object RunAuditTests extends ZIOSpecDefault with ApplicationLogger {

  override val bootstrap: ULayer[TestEnvironment] = testEnvironment ++ zioSlf4jLogger

  private val env = audit.BQ() ++ BQ.live()

  val initBQ: Spec[BQ, Any] =
    suite("InitBQTest")(
      test("InitBQTest") {
        assertZIO(program.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      }
    )

  override def spec: Spec[TestEnvironment, Any] = (suite("BQ Audit")(
    audit.SqlTestSuite.spec,
    initBQ,
    audit.BQTestSuite.spec
  ) @@ TestAspect.sequential).provideShared(env.orDie)
}
