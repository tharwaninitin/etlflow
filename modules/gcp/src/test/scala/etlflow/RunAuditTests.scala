package etlflow

import etlflow.audit.CreateBQ.program
import etlflow.log.ApplicationLogger
import zio.ULayer
import zio.test._

object RunAuditTests extends ZIOSpecDefault with ApplicationLogger {

  override val bootstrap: ULayer[TestEnvironment] = testEnvironment ++ zioSlf4jLogger

  override def spec: Spec[TestEnvironment, Any] = (suite("BQ Audit")(
    audit.SqlTestSuite.spec,
    test("InitBQ")(program.as(assertCompletes)),
    audit.BQTestSuite.spec
  ) @@ TestAspect.sequential).provideShared(audit.BQ().orDie)
}
