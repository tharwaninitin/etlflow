package etlflow.coretests.executor

import etlflow.EtlJobProps
import etlflow.coretests.MyEtlJobPropsMapping
import etlflow.coretests.steps.DBStepTestSuite.fullLayer
import etlflow.etljobs.{EtlJob => CoreEtlJob}
import etlflow.executor.{LocalExecutor, LocalSubProcessExecutor}
import etlflow.schema.Executor.LOCAL_SUBPROCESS
import etlflow.utils.{ReflectAPI => RF}
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object LocalSubProcessExecutorTestSuite  extends DefaultRunnableSpec  {


  val local_subprocess = LOCAL_SUBPROCESS("universal/scripts/bin/examples",heap_min_memory = "-Xms100m",heap_max_memory = "-Xms100m")

  val localJob1 = LocalSubProcessExecutor(local_subprocess).executeJob("Job8", Map.empty)
  val localJob2 = LocalSubProcessExecutor(local_subprocess).executeJob("Job8", Map("path" -> "abc"))

  override def spec: ZSpec[environment.TestEnvironment, Any] =
    (suite(" Local Sub Process Executor Spec")(
      testM("local_subprocess Job1 ") {
        assertM(localJob1.foldM(ex => ZIO.succeed("ok"), _ => ZIO.succeed("Done")))(equalTo("ok"))
      },
      testM("local_subprocess Job2") {
        assertM(localJob2.foldM(ex => ZIO.succeed("ok"), _ => ZIO.succeed("Done")))(equalTo("ok"))
      }
    )@@ TestAspect.sequential).provideCustomLayer(fullLayer.orDie)
}
