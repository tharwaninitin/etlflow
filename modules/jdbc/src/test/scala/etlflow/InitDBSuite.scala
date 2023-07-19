package etlflow

import etlflow.audit.CreateDB
import etlflow.db.DB
import zio.test.Assertion.equalTo
import zio.test._
import zio.{RIO, ZIO}

object InitDBSuite extends DbSuiteHelper {
  val sql: String =
    """
      |INSERT INTO jobrun (job_run_id,job_name,metadata,status,created_at,updated_at)
      |VALUES ('a27a7415-57b2-4b53-8f9b-5254e847a301','EtlJobDownload','{}','pass',current_timestamp(6),current_timestamp(6));
      |INSERT INTO jobrun (job_run_id,job_name,metadata,status,created_at,updated_at)
      |VALUES ('a27a7415-57b2-4b53-8f9b-5254e847a302','EtlJobSpr','{}','pass',current_timestamp(6),current_timestamp(6));
      |INSERT INTO taskrun (task_run_id,job_run_id,task_name,task_type,metadata,status,created_at,updated_at)
      |VALUES ('123','a27a7415-57b2-4b53-8f9b-5254e847a301','download_spr','GenericEtlTask','{}','pass',current_timestamp(6),current_timestamp(6));
      |""".stripMargin

  def program(reset: Boolean): RIO[DB, Unit] = for {
    _ <- CreateDB.execute(reset)
    _ <- DB.executeQuery(sql)
  } yield ()

  def spec(reset: Boolean): Spec[DB, Any] =
    suite("InitTestDB")(
      test("InitTestDB") {
        assertZIO(program(reset).foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      }
    )
}
