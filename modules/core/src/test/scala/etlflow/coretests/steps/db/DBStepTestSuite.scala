package etlflow.coretests.steps.db

import etlflow.coretests.Schema.EtlJobRun
import etlflow.coretests.TestSuiteHelper
import etlflow.etlsteps.{DBQueryStep, DBReadStep}
import etlflow.schema.Credential.JDBC
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test.{DefaultRunnableSpec, ZSpec, assertM, environment, _}


object DBStepTestSuite extends DefaultRunnableSpec with TestSuiteHelper {

  val step2 = DBQueryStep(
    name  = "UpdatePG",
    query = "BEGIN; DELETE FROM ratings_par WHERE 1 = 1; COMMIT;",
    credentials = JDBC(config.db.url, config.db.user, config.db.password, "org.postgresql.Driver")
  )

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow")(
      suite("DB Steps")(
        testM("Execute DB step") {
          val create_table_script = """
              CREATE TABLE IF NOT EXISTS ratings_par (
                user_id int
              , movie_id int
              , rating int
              , timestamp int
              , date date
              )
              """
          val step1 = DBQueryStep(
            name  = "UpdatePG",
            query = create_table_script,
            credentials = JDBC(config.db.url, config.db.user, config.db.password, "org.postgresql.Driver")
          )
          val step2 = DBQueryStep(
            name  = "UpdatePG",
            query = "BEGIN; DELETE FROM ratings_par WHERE 1 = 1; COMMIT;",
            credentials = JDBC(config.db.url, config.db.user, config.db.password, "org.postgresql.Driver")
          )
          val step3 = DBReadStep[EtlJobRun](
            name  = "FetchEtlJobRun",
            query = "SELECT job_name,job_run_id,state FROM jobrun LIMIT 10",
            credentials = JDBC(config.db.url, config.db.user, config.db.password, "org.postgresql.Driver")
          )
          val job = for {
            _ <- step1.process().provideCustomLayer(fullLayer)
            _ <- step2.process().provideCustomLayer(fullLayer)
            _ <- step3.process().provideCustomLayer(fullLayer)
          } yield ()
          assertM(job.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
        },
        test("Execute getStepProperties") {
          val props = step2.getStepProperties()
          assert(props)(equalTo(Map("query" -> "BEGIN; DELETE FROM ratings_par WHERE 1 = 1; COMMIT;")))
        }
      )
    )
}


