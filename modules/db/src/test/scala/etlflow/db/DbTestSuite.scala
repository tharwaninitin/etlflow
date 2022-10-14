package etlflow.db

import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

@SuppressWarnings(Array("org.wartremover.warts.JavaSerializable", "org.wartremover.warts.Serializable"))
object DbTestSuite {

  case class TestDb(name: String)

  val spec: Spec[TestEnvironment with DBEnv, Any] =
    suite("DB Suite")(
      test("executeQuery Test") {
        val query =
          """BEGIN;
               DROP TABLE IF EXISTS jobrun1;
               CREATE TABLE jobrun1 as SELECT * FROM jobrun;
             COMMIT;
          """.stripMargin
        assertZIO(DBApi.executeQuery(query).foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      },
      test("executeQuerySingleOutput Test")(
        assertZIO(
          DBApi
            .executeQuerySingleOutput("""SELECT job_name FROM jobrun ORDER BY job_run_id LIMIT 1""")(rs => rs.string("job_name"))
            .foldZIO(ex => ZIO.fail(ex.getMessage), op => ZIO.succeed(op))
        )(equalTo("EtlJobDownload"))
      ),
      test("executeQueryListOutput Test") {
        val res = DBApi
          .executeQueryListOutput[TestDb]("SELECT job_name FROM jobrun")(rs => TestDb(rs.string("job_name")))
          .foldZIO(_ => ZIO.fail(List.empty[TestDb]), op => ZIO.succeed(op))
        assertZIO(res)(equalTo(List(TestDb("EtlJobDownload"), TestDb("EtlJobSpr"))))
      }
    ) @@ TestAspect.sequential
}
