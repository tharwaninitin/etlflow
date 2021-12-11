package examples.jobs

import etlflow.etljobs.EtlJob
import etlflow.etlsteps.{BQQueryStep, DBQueryStep}
import etlflow.utils.Configuration
import examples.schema.MyEtlJobProps.EtlJob4Props

case class EtlJobDbQueryStep(job_properties: EtlJob4Props) extends EtlJob[EtlJob4Props]{

  val config = zio.Runtime.default.unsafeRun(Configuration.config)

  private val query1 = """CREATE OR REPLACE PROCEDURE dev_reports.sp_temp_delete(user_id INT64)
                         |BEGIN
                         |  DECLARE count_user INT64 DEFAULT 0;
                         |  SET count_user =(SELECT COUNT(*) FROM dev.ratings WHERE userId = user_id);
                         |  SELECT count_user ;
                         |END""".stripMargin


  private val step1 = BQQueryStep(
    name = "CreateStoredProcedure1BQ",
    query = query1
  )

  private val step2 = BQQueryStep(
    name = "RunStoredProcedure2BQ",
    query = "CALL dev_reports.sp_temp_delete(361)"
  )

  private val step3 = BQQueryStep(
    name  = "CreateTableBQ",
    query = s"""CREATE OR REPLACE TABLE dev.ratings_grouped as
            SELECT movieId, COUNT(1) cnt
            FROM dev.ratings
            GROUP BY movieId
            ORDER BY cnt DESC;""".stripMargin
  )

  private val step4 = DBQueryStep(
    name  = "UpdatePG",
    query = "BEGIN; DELETE FROM ratings WHERE 1 =1; INSERT INTO ratings SELECT * FROM ratings_temp; COMMIT;",
    credentials = config.db.get
  )

  override val job = for {
    -  <- step1.execute(())
    -  <- step2.execute(())
    -  <- step3.execute(())
    -  <- step4.execute(())
  } yield ()
}
