package examples.jobs

import com.google.cloud.bigquery.JobInfo
import etljobs.EtlStepList
import etljobs.etljob.SequentialEtlJob
import etljobs.etlsteps.{BQLoadStep, BQQueryStep, EtlStep}
import etljobs.utils.BQ
import examples.MyGlobalProperties
import examples.schema.MyEtlJobProps

case class EtlJob4Definition(job_properties: MyEtlJobProps, global_properties: Option[MyGlobalProperties]) extends SequentialEtlJob {

  private val select_query: String = """
      | SELECT movie_id, COUNT(1) cnt
      | FROM test.ratings
      | GROUP BY movie_id
      | ORDER BY cnt DESC;
      |""".stripMargin
  private val getQuery: String => String = param => s"""
     | SELECT date, movie_id, COUNT(1) cnt
     | FROM test.ratings_par
     | WHERE date = '$param'
     | GROUP BY date, movie_id
     | ORDER BY cnt DESC;
     |""".stripMargin
  private val input_query_partitions = Seq(
    (getQuery("2016-01-01"),"20160101"),
    (getQuery("2016-01-02"),"20160102")
  )

  // val query = "CREATE OR REPLACE TABLE test.ratings_temp (a INT64)"
  private val query1 = """CREATE OR REPLACE PROCEDURE test_reports.sp_temp_delete(start_date DATE)
                |BEGIN
                |  DECLARE count_dt INT64 DEFAULT 0;
                |  SET count_dt =(SELECT COUNT(*) FROM test.ratings WHERE date = start_date);
                |  SELECT count_dt;
                |END""".stripMargin
  private val query2 = "CALL test_reports.sp_temp_delete('2016-01-01')"

  private val step1 = BQQueryStep(
    name = "CreateStoredProcedure",
    query = query1
  )

  private val step2 = BQQueryStep(
      name = "CreateStoredProcedure",
      query = query2
  )

  private val step3 = BQLoadStep(
    name            = "LoadQueryDataBQ",
    input_location  = Left(select_query),
    input_type      = BQ,
    output_dataset  = "test",
    output_table    = "ratings_grouped",
    output_create_disposition = JobInfo.CreateDisposition.CREATE_IF_NEEDED
  )

  private val step4 = BQLoadStep(
    name           = "LoadQueryDataBQPar",
    input_location = Right(input_query_partitions),
    input_type     = BQ,
    output_dataset = "test",
    output_table   = "ratings_grouped_par"
  )

  val etl_step_list: List[EtlStep[_,_]] = EtlStepList(step1,step2,step3,step4)
}
