package etlflow.etlsteps

import etlflow.schema.Config
import etlflow.schema.Credential.JDBC
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test.{ZSpec, assertM, assert, environment, suite, test, testM}

case class DBStepTestSuite(config: Config) {

  val step2 = DBQueryStep(
    name = "UpdatePG",
    query = "BEGIN; DELETE FROM ratings_par WHERE 1 = 1; COMMIT;",
    credentials = JDBC(config.db.get.url, config.db.get.user, config.db.get.password, "org.postgresql.Driver")
  )

  case class EtlJobRun(job_name: String, job_run_id: String, state: String)

  val spec: ZSpec[environment.TestEnvironment, Any] =
    suite("DB Steps")(
      testM("Execute DB steps") {
        val create_table_script =
          """
            CREATE TABLE IF NOT EXISTS ratings_par (
              user_id int
            , movie_id int
            , rating int
            , timestamp int
            , date date
            )
            """
        val step1 = DBQueryStep(
          name = "UpdatePG",
          query = create_table_script,
          credentials = JDBC(config.db.get.url, config.db.get.user, config.db.get.password, "org.postgresql.Driver")
        )
        val step2 = DBQueryStep(
          name = "UpdatePG",
          query = "BEGIN; DELETE FROM ratings_par WHERE 1 = 1; COMMIT;",
          credentials = JDBC(config.db.get.url, config.db.get.user, config.db.get.password, "org.postgresql.Driver")
        )
        val step3 = DBReadStep[EtlJobRun](
          name = "FetchEtlJobRun",
          query = "SELECT job_name,job_run_id,status FROM jobrun LIMIT 10",
          credentials = JDBC(config.db.get.url, config.db.get.user, config.db.get.password, "org.postgresql.Driver")
        )(rs => EtlJobRun(rs.string("job_name"), rs.string("job_run_id"), rs.string("status")))

        val job = for {
          _ <- step1.process(())
          _ <- step2.process(())
          _ <- step3.process(())
        } yield ()
        assertM(job.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      test("Execute getStepProperties") {
        val props = step2.getStepProperties
        assert(props)(equalTo(Map("query" -> "BEGIN; DELETE FROM ratings_par WHERE 1 = 1; COMMIT;")))
      }
    )
}
