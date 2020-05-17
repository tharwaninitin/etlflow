package examples.jobs

import com.google.cloud.bigquery.JobInfo
import etlflow.EtlStepList
import etlflow.etljobs.SequentialEtlJob
import etlflow.etlsteps.{BQLoadStep, BQQueryStep, EtlStep}
import etlflow.utils.BQ
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

  private val step1 = BQLoadStep(
    name            = "LoadQueryDataBQ",
    input_location  = Left(select_query),
    input_type      = BQ,
    output_dataset  = "test",
    output_table    = "ratings_grouped",
    output_create_disposition = JobInfo.CreateDisposition.CREATE_IF_NEEDED
  )

  private val step2 = BQLoadStep(
    name           = "LoadQueryDataBQPar",
    input_location = Right(input_query_partitions),
    input_type     = BQ,
    output_dataset = "test",
    output_table   = "ratings_grouped_par"
  )

  val etlStepList: List[EtlStep[_,_]] = EtlStepList(step1,step2)
}
