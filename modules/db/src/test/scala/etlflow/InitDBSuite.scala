package etlflow

import etlflow.db.utils.CreateDB
import etlflow.db.{DBApi, DBEnv}
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object InitDBSuite extends DbSuiteHelper {
  val sql: String =
    """
      |INSERT INTO jobrun (job_run_id,job_name,properties,status,elapsed_time,job_type,is_master,inserted_at)
      |VALUES ('a27a7415-57b2-4b53-8f9b-5254e847a301','EtlJobDownload','{}','pass','','GenericEtlJob','true',1234567);
      |INSERT INTO jobrun (job_run_id,job_name,properties,status,elapsed_time,job_type,is_master,inserted_at)
      |VALUES ('a27a7415-57b2-4b53-8f9b-5254e847a302','EtlJobSpr','{}','pass','','GenericEtlJob','true',1234567);
      |INSERT INTO taskrun (task_run_id,job_run_id,task_name,properties,status,elapsed_time,task_type,inserted_at)
      |VALUES ('123','a27a7415-57b2-4b53-8f9b-5254e847a301','download_spr','{}','pass','1.6 mins','GenericEtlTask',1234567);
      |""".stripMargin

  val program: ZIO[DBEnv, Throwable, Unit] = for {
    _ <- CreateDB(true)
    _ <- DBApi.executeQuery(sql)
  } yield ()

  val spec: ZSpec[environment.TestEnvironment with DBEnv, Any] =
    suite("InitTestDB")(
      testM("InitTestDB") {
        assertM(program.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      }
    )
}
