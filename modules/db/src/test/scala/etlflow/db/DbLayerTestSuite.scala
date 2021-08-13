package etlflow.db

import etlflow.DbSuiteHelper
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object DbLayerTestSuite extends DefaultRunnableSpec with DbSuiteHelper {

  val jobDbAll = List(JobDBAll("Job1","","",0,0,true,None), JobDBAll("Job2","","",0,0,false,None), JobDBAll("Job3","","",0,0,true,None))
  val stepRuns = List(
      StepRun("a27a7415-57b2-4b53-8f9b-5254e847a301","download_spr","{}","pass","1970-01-01 00:20:34 UTC","1.6 mins","GenericEtlStep","123")
    )
  val jobRuns = List(
      JobRun("a27a7415-57b2-4b53-8f9b-5254e847a301","EtlJobDownload","{}","pass","1970-01-01 00:20:34 UTC","","GenericEtlJob","true"),
      JobRun("a27a7415-57b2-4b53-8f9b-5254e847a302","EtlJobSpr","{}","pass","1970-01-01 00:20:34 UTC","","GenericEtlJob","true")
    )
  case class TestDb(name:String)
  val jobLogs = List(JobLogs("EtlJobDownload","1","0"), JobLogs("EtlJobSpr","1","0"))
  val getCredential = List(GetCredential("AWS", "JDBC", "2021-07-21 12:37:19.298812"))

  override def spec: ZSpec[environment.TestEnvironment, Any] =
    (suite("Implementation Suite")(
      testM("getUser Test")(
        assertM(DBApi.getUser("admin").map(x => x.user_name))(equalTo("admin"))
      ),
      testM("getJob Test")(
        assertM(DBApi.getJob("Job1").map(x => x))(equalTo(JobDB("Job1","",true)))
      ),
      testM("getJobs Test")(
        assertM(DBApi.getJobs.map(x => x))(equalTo(jobDbAll))
      ),
      testM("getStepRuns Test")(
        assertM(DBApi.getStepRuns(DbStepRunArgs("a27a7415-57b2-4b53-8f9b-5254e847a301")).map(x => x.sortBy(_.job_run_id)))(equalTo(stepRuns.sortBy(_.job_run_id)))
      ),
      testM("getJobRuns Test")(
        assertM(DBApi.getJobRuns(DbJobRunArgs(None, None,None,None,None,10L,0L)).map(x => x.sortBy(_.job_name)))(equalTo(jobRuns.sortBy(_.job_name)))
      ),
      testM("getJobLogs Test")(
        assertM(DBApi.getJobLogs(JobLogsArgs(None,Some(10L))).map(x => x.sortBy(_.job_name)))(equalTo(jobLogs.sortBy(_.job_name)))
      ),
      testM("getCredentials Test")(
        assertM(DBApi.getCredentials.map(x => x))(equalTo(getCredential))
      ),
      testM("updateSuccessJob Test")(
        assertM(DBApi.updateSuccessJob("Job1", 1L).map(x => x))(equalTo(1L))
      ),
      testM("updateFailedJob Test")(
        assertM(DBApi.updateFailedJob("Job1", 1L).map(x => x))(equalTo(1L))
      ),
      testM("updateJobState Test")(
        assertM(DBApi.updateJobState(EtlJobStateArgs("Job1",true)).map(x => x))(equalTo(true))
      ),
      testM("addCredential Test")(
        assertM(DBApi.addCredential(CredentialDB("AWS1","JDBC",JsonString("{}")), JsonString("{}")).map(x => x))(equalTo(Credentials("AWS1","JDBC",JsonString("{}").str)))
      ),
      testM("updateCredential Test")(
        assertM(DBApi.updateCredential(CredentialDB("AWS1","JDBC",JsonString("{}")), JsonString("{}")).map(x => x))(equalTo(Credentials("AWS1","JDBC",JsonString("{}").str)))
      ),
      testM("updateStepRun Test")(
        assertM(DBApi.updateStepRun("a27a7415-57b2-4b53-8f9b-5254e847a301", "download_spr", "{}", "pass", "").foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      ),
      testM("insertStepRun Test")(
        assertM(DBApi.insertStepRun("a27a7415-57b2-4b53-8f9b-5254e847a3011", "download_spr", "{}", "Generic", "123", 0L).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      ),
      testM("insertJobRun Test")(
        assertM(DBApi.insertJobRun("a27a7415-57b2-4b53-8f9b-5254e847a3011", "download_spr", "{}", "Generic", "true", 0L).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      ),
      testM("updateJobRun Test")(
        assertM(DBApi.updateJobRun("a27a7415-57b2-4b53-8f9b-5254e847a3011", "pass", "").foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      ),
      testM("refreshJobs Test")(
        assertM(DBApi.refreshJobs(List(EtlJob("Job1",Map.empty),EtlJob("Job2",Map.empty))).map(x => x))(equalTo(List(JobDB("Job1","",true),JobDB("Job2","",false))))
      ),
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
               DELETE FROM jobrun;
               INSERT INTO jobrun SELECT * FROM jobrun1 LIMIT 1;
             COMMIT;
          """.stripMargin
        assertM(DBApi.executeQuery(query).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      }),
      testM("executeQueryListOutput Test")({
        val res: ZIO[DBEnv, Throwable, List[TestDb]] = DBApi.executeQueryListOutput[TestDb]("SELECT job_name FROM job")(rs => TestDb(rs.string("job_name")))
        assertM(res)(equalTo(List(TestDb("Job1"),TestDb("Job2"))))
      })
    ) @@ TestAspect.sequential).provideCustomLayerShared(liveDB(credentials).orDie)
}