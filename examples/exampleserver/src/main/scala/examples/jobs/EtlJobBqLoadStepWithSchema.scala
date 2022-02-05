package examples.jobs

import com.google.cloud.bigquery.Schema
import etlflow.etljobs.EtlJob
import etlflow.etlsteps.BQLoadStep
import etlflow.log.LogEnv
import examples.schema.MyEtlJobProps.EtlJob2Props
import examples.schema.MyEtlJobSchema.RatingBQ
import gcp4zio.{BQ, BQInputType}
import utils.Encoder

case class EtlJobBqLoadStepWithSchema(job_properties: EtlJob2Props) extends EtlJob[EtlJob2Props] {
  private val job_props = job_properties

  val schema: Option[Schema] = Encoder[RatingBQ]
  private val step1 = BQLoadStep(
    name = "LoadRatingBQ",
    input_location = Left(job_properties.ratings_input_path + "/" + job_properties.ratings_output_file_name.get),
    input_type = BQInputType.CSV(),
    output_dataset = job_props.ratings_output_dataset,
    output_table = job_props.ratings_output_table_name,
    schema = schema
  ).execute.provideSomeLayer[LogEnv](BQ.live())

  val job = step1
}
