package etljobs.examples.etljob4

import com.google.cloud.bigquery.JobInfo
import etljobs.{EtlJob, EtlJobProps, EtlStepList}
import etljobs.bigquery.BigQueryManager
import etljobs.etlsteps.{BQLoadStep, BQQueryStep, StateLessEtlStep}
import etljobs.utils.{BQ, GlobalProperties}

case class EtlJobDefinition(job_name: String,job_properties: EtlJobProps, global_properties: Option[GlobalProperties] = None)
  extends EtlJob with BigQueryManager {

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
  // val query = "CALL test_reports.sp_temp_delete('2016-01-01')"
  private val query = """CREATE PROCEDURE test_reports.sp_temp_delete(start_date DATE)
                |BEGIN
                |  DECLARE count_dt INT64 DEFAULT 0;
                |  SET count_dt =(SELECT COUNT(*) FROM test.ratings WHERE date = start_date);
                |  SELECT count_dt;
                |END""".stripMargin

  private val step1 = BQQueryStep(
    name = "CreateStoredProcedure",
    query = query
  )(bq)

  private val step2 = BQQueryStep(
    name = "CreateStoredProcedure",
    query = query
  )(bq)

  private val step3 = BQLoadStep(
    name            = "LoadQueryDataBQ",
    input_location  = Left(select_query),
    input_type      = BQ,
    output_dataset  = "test",
    output_table    = "ratings_grouped",
    output_create_disposition = JobInfo.CreateDisposition.CREATE_IF_NEEDED
  )(bq)

  private val step4 = BQLoadStep(
    name           = "LoadQueryDataBQPar",
    input_location = Right(input_query_partitions),
    input_type     = BQ,
    output_dataset = "test",
    output_table   = "ratings_grouped_par"
  )(bq)

  val etl_step_list: List[StateLessEtlStep] = EtlStepList(step1,step2,step3,step4)
}
