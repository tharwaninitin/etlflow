package etlflow.db

import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object DbTestSuite {

  case class TestDb(name: String)

  val spec: ZSpec[environment.TestEnvironment with DBEnv, Any] =
    suite("DB Suite")(
      testM("executeQuery Test") {
        val query =
          """BEGIN;
               CREATE TABLE jobrun1 as SELECT * FROM jobrun;
             COMMIT;
          """.stripMargin
        assertM(DBApi.executeQuery(query).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      },
      testM("executeQuerySingleOutput Test")(
        assertM(
          DBApi
            .executeQuerySingleOutput("""SELECT job_name FROM jobrun ORDER BY job_run_id LIMIT 1""")(rs => rs.string("job_name"))
            .foldM(ex => ZIO.fail(ex.getMessage), op => ZIO.succeed(op))
        )(equalTo("EtlJobDownload"))
      ),
      testM("executeQueryListOutput Test") {
        val res: ZIO[DBEnv, Throwable, List[TestDb]] =
          DBApi.executeQueryListOutput[TestDb]("SELECT job_name FROM jobrun")(rs => TestDb(rs.string("job_name")))
        assertM(res)(equalTo(List(TestDb("EtlJobDownload"), TestDb("EtlJobSpr"))))
      }
    ) @@ TestAspect.sequential
}
