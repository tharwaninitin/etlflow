package etlflow.db

import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object DbTestSuite {

  case class TestDb(name:String)

  val spec: ZSpec[environment.TestEnvironment with DBEnv, Any] =
    suite("DB Suite")(
      testM("executeQuery Test")(
        assertM(DBApi.executeQuery("""INSERT INTO userinfo(user_name,password,user_active,user_role) values ('admin1','admin',true,'admin')""").foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      ),
      testM("executeQuerySingleOutput Test")(
        assertM(DBApi.executeQuerySingleOutput("""SELECT user_name FROM userinfo LIMIT 1""")(rs => rs.string("user_name")).foldM(ex => ZIO.fail(ex.getMessage), op => ZIO.succeed(op)))(equalTo("admin"))
      ),
      testM("executeQueryListOutput Test with pg syntax")({
        val query =
          """BEGIN;
               CREATE TABLE jobrun1 as SELECT * FROM jobrun;
             COMMIT;
          """.stripMargin
        assertM(DBApi.executeQuery(query).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      }),
      testM("executeQueryListOutput Test")({
        val res: ZIO[DBEnv, Throwable, List[TestDb]] = DBApi.executeQueryListOutput[TestDb]("SELECT job_name FROM job")(rs => TestDb(rs.string("job_name")))
        assertM(res)(equalTo(List(TestDb("Job1"),TestDb("Job2"))))
      })
    ) @@ TestAspect.sequential
}