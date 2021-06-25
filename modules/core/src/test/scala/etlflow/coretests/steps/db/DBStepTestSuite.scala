package etlflow.coretests.steps.db

import etlflow.coretests.TestSuiteHelper
import etlflow.db.liveDBWithTransactor
import etlflow.etlsteps.DBQueryStep
import etlflow.schema.Credential.JDBC
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test.{DefaultRunnableSpec, ZSpec, assertM, environment}

object DBStepTestSuite extends DefaultRunnableSpec with TestSuiteHelper {

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
            credentials = JDBC(config.dbLog.url, config.dbLog.user, config.dbLog.password, "org.postgresql.Driver")
          )
          val step2 = DBQueryStep(
            name  = "UpdatePG",
            query = "BEGIN; DELETE FROM ratings_par WHERE 1 = 1; COMMIT;",
            credentials = JDBC(config.dbLog.url, config.dbLog.user, config.dbLog.password, "org.postgresql.Driver")
          )
          val job = for {
            _ <- step1.process().provideCustomLayer(fullLayer)
            _ <- step2.process().provideCustomLayer(fullLayer)
          } yield ()
          assertM(job.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
        }
      )
    )
}


