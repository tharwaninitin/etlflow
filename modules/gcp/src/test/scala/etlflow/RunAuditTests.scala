package etlflow

import etlflow.audit.CreateBQ.program
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object RunAuditTests extends ZIOSpecDefault with TestHelper {

  private val jri = "a27a7415-57b2-4b53-8f9b-5254e847a3011"

  private val env = audit.BQ(jri)

  val initBQ: Spec[Any, Any] =
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
