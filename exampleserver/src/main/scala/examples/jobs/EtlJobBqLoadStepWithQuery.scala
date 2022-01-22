package examples.jobs

import com.google.cloud.bigquery.JobInfo
import etlflow.etljobs.EtlJob
import etlflow.etlsteps.BQLoadStep
import etlflow.gcp.BQInputType
import examples.schema.MyEtlJobProps.EtlJob6Props

case class EtlJobBqLoadStepWithQuery(job_properties: EtlJob6Props) extends EtlJob[EtlJob6Props] {

  private val select_query: String =
    """
      | SELECT movieId, COUNT(1) cnt
      | FROM dev.ratings
      | GROUP BY movieId
      | ORDER BY cnt DESC;
      |""".stripMargin

  private val step1 = BQLoadStep(
    name = "LoadQueryDataBQ",
    input_location = Left(select_query),
    input_type = BQInputType.BQ,
    output_dataset = "dev",
    output_table = "ratings_grouped",
    output_create_disposition = JobInfo.CreateDisposition.CREATE_IF_NEEDED
  )

  val job = step1.execute
}
