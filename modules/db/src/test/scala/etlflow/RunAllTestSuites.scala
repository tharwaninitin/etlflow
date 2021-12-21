package etlflow

import etlflow.db._
import zio.test._

object RunAllTestSuites extends DefaultRunnableSpec with DbSuiteHelper {
  val sql =
    """
      |DROP TABLE IF EXISTS jobrun1;
      |
      |INSERT INTO userinfo (user_name,password,user_active,user_role)
      |VALUES ('admin','$2a$10$gABYeKWB2W0nI.zGCoovD.7emHUlHq1flgxWjqAIowdLMWkzYlIOy','true','admin');
      |
      |INSERT INTO job(job_name,job_description,schedule,failed,success,is_active)
      |VALUES ('Job1','','',0,0,'t');
      |INSERT INTO job(job_name,job_description,schedule,failed,success,is_active)
      |VALUES ('Job2','','',0,0,'f');
      |INSERT INTO job(job_name,job_description,schedule,failed,success,is_active)
      |VALUES ('Job3','','',0,0,'t');
      |
      |INSERT INTO jobrun (job_run_id,job_name,properties,status,elapsed_time,job_type,is_master,inserted_at)
      |VALUES ('a27a7415-57b2-4b53-8f9b-5254e847a301','EtlJobDownload','{}','pass','','GenericEtlJob','true',1234567);
      |INSERT INTO jobrun (job_run_id,job_name,properties,status,elapsed_time,job_type,is_master,inserted_at)
      |VALUES ('a27a7415-57b2-4b53-8f9b-5254e847a302','EtlJobSpr','{}','pass','','GenericEtlJob','true',1234567);
      |
      |INSERT INTO steprun (step_run_id,job_run_id,step_name,properties,status,elapsed_time,step_type,inserted_at)
      |VALUES ('123','a27a7415-57b2-4b53-8f9b-5254e847a301','download_spr','{}','pass','1.6 mins','GenericEtlStep',1234567);
      |
      |INSERT INTO credential(name, type, value, valid_from)
      |VALUES('AWS','JDBC','{}','2021-07-21 12:37:19.298812');
      |""".stripMargin
  zio.Runtime.default.unsafeRun(DBApi.executeQuery(sql).provideLayer(db.liveDB(credentials)))
  def spec: ZSpec[environment.TestEnvironment, Any] = suite("Db Test Suites") (
    DbTestSuite.spec,
    SqlTestSuite.spec,
    UtilsTestSuite.spec
  ).provideCustomLayerShared(liveFullDB(credentials).orDie)
}
