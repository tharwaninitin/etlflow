package etlflow.coretests.steps.master

import etlflow.coretests.Schema.EtlJob1Props
import etlflow.etlsteps.EtlFlowJobStep
import etlflow.coretests.jobs.Job1HelloWorld
import etlflow.jdbc.liveDBWithTransactor
import etlflow.utils.{Configuration, DbManager}
import zio.ZIO
import zio.test.Assertion._
import zio.test._

object EtlFlowJobStepTestSuite extends DefaultRunnableSpec with Configuration with DbManager {
  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlowJobStepTestSuite") (
      testM("Execute EtlFlowJobStep") {
        val dbLayer = liveDBWithTransactor(config.dbLog)
        val step = EtlFlowJobStep[EtlJob1Props](
          name = "Test",
          job  = Job1HelloWorld(EtlJob1Props()),
        ).process().provideCustomLayer(dbLayer)
        assertM(step.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
  )
}
