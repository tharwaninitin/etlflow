package etlflow.executor

import etlflow.etljobs.EtlJob
import etlflow.jobtests.MyEtlJobPropsMapping
import etlflow.schema.Config
import etlflow.{EtlJobProps, JobEnv}
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

case class LocalExecutorTestSuite(config: Config) {
  type MEJP = MyEtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]
  val jobProps = LocalExecutor[MEJP]().showJobProps("Job1")
  val jobActualProps = LocalExecutor[MEJP]().getActualJobProps("Job2", Map("ratings_output_table_name" -> "test"))
  val executeJob = LocalExecutor[MEJP]().executeJob("Job1", Map.empty, config, java.util.UUID.randomUUID.toString)

  val spec: ZSpec[environment.TestEnvironment with JobEnv, Any] =
    suite("Local Executor")(
      testM("showJobProps") {
        assertM(jobProps.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      },
      testM("getActualJobProps") {
        val input_file_path = s"${new java.io.File(".").getCanonicalPath}/modules/core/src/test/resources/input/movies/ratings_parquet/ratings.parquet"
        val op = s"""{"ratings_input_path":["$input_file_path"],"ratings_output_table_name":"test"}"""
        assertM(jobActualProps.foldM(ex => ZIO.fail(ex.getMessage), props => ZIO.succeed(props)))(equalTo(op))
      },
      testM("executeJob") {
        assertM(executeJob.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      }
    ) @@ TestAspect.sequential
}
