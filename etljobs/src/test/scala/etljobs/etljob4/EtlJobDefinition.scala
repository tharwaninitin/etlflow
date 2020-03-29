package etljobs.etljob4

import com.google.cloud.bigquery.JobInfo
import etljobs.{EtlJob, EtlJobProps, EtlStepList}
import etljobs.bigquery.BigQueryManager
import etljobs.etlsteps.{BQLoadStep, StateLessEtlStep}
import etljobs.utils.{BQ, GlobalProperties}

case class EtlJobDefinition(job_properties: EtlJobProps, global_properties: Option[GlobalProperties] = None)
  extends EtlJob with BigQueryManager {

  val query = """
      | SELECT movie_id, COUNT(1) cnt
      | FROM test.ratings
      | GROUP BY movie_id
      | ORDER BY cnt DESC;
      |""".stripMargin
  val getQuery: String => String = param => s"""
     | SELECT date, movie_id, COUNT(1) cnt
     | FROM test.ratings_par
     | WHERE date = '$param'
     | GROUP BY date, movie_id
     | ORDER BY cnt DESC;
     |""".stripMargin
  val input_query_partitions = Seq(
    (getQuery("2016-01-01"),"20160101"),
    (getQuery("2016-01-02"),"20160102")
  )

  val step1 = BQLoadStep(
    name            = "LoadQueryDataBQ",
    input_location  = Left(query),
    input_type      = BQ,
    output_dataset  = "test",
    output_table    = "ratings_grouped",
    output_create_disposition = JobInfo.CreateDisposition.CREATE_IF_NEEDED
  )(bq)

  val step2 = BQLoadStep(
    name           = "LoadQueryDataBQPar",
    input_location = Right(input_query_partitions),
    input_type     = BQ,
    output_dataset = "test",
    output_table   = "ratings_grouped_par"
  )(bq)

  val etl_step_list: List[StateLessEtlStep] = EtlStepList(step1,step2)
}
