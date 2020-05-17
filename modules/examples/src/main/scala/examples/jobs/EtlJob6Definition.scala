package examples.jobs

import etljobs.EtlStepList
import etljobs.etljob.SequentialEtlJob
import etljobs.etlsteps.{BQQueryStep, DBQueryStep, EtlStep}
import etljobs.utils.JDBC
import examples.MyGlobalProperties
import examples.schema.MyEtlJobProps

case class EtlJob6Definition(job_properties: MyEtlJobProps, global_properties: Option[MyGlobalProperties]) extends SequentialEtlJob {
  private val global_props = global_properties.get

  private val query1 = """CREATE OR REPLACE PROCEDURE test_reports.sp_temp_delete(start_date DATE)
                         |BEGIN
                         |  DECLARE count_dt INT64 DEFAULT 0;
                         |  SET count_dt =(SELECT COUNT(*) FROM test.ratings WHERE date = start_date);
                         |  SELECT count_dt;
                         |END""".stripMargin


  private val step1 = BQQueryStep(
    name = "CreateStoredProcedure1BQ",
    query = query1
  )

  private val step2 = BQQueryStep(
    name = "RunStoredProcedure2BQ",
    query = "CALL test_reports.sp_temp_delete('2016-01-01')"
  )

  private val step3 = BQQueryStep(
    name  = "CreateTableBQ",
    query = s"""CREATE OR REPLACE TABLE test.ratings_grouped as
            SELECT movie_id, COUNT(1) cnt
            FROM test.ratings
            GROUP BY movie_id
            ORDER BY cnt DESC;""".stripMargin
  )

  private val step4 = DBQueryStep(
    name  = "UpdatePG",
    query = "BEGIN; DELETE FROM ratings WHERE 1 =1; INSERT INTO ratings SELECT * FROM ratings_temp; COMMIT;",
    credentials = JDBC(global_props.log_db_url, global_props.log_db_user, global_props.log_db_pwd, global_props.jdbc_driver)
  )

  val etlStepList: List[EtlStep[_,_]] = EtlStepList(step1,step2,step3,step4)
}
