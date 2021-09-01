package examples.jobs

import com.google.cloud.bigquery.Schema
import etlflow.etljobs.GenericEtlJob
import etlflow.etlsteps.BQLoadStep
import etlflow.gcp.{BQInputType, getBqSchema}
import examples.schema.MyEtlJobProps.EtlJob2Props
import examples.schema.MyEtlJobSchema.RatingBQ

case class EtlJobBqLoadStepWithSchema(job_properties: EtlJob2Props) extends GenericEtlJob[EtlJob2Props] {
  private val job_props = job_properties

  val schema: Option[Schema] = getBqSchema[RatingBQ]
  private val step1 = BQLoadStep(
    name           = "LoadRatingBQ",
    input_location = Left(job_properties.ratings_input_path + "/" + job_properties.ratings_output_file_name.get),
    input_type     = BQInputType.CSV(),
    output_dataset = job_props.ratings_output_dataset,
    output_table   = job_props.ratings_output_table_name,
    schema = schema
  )

  val job = for {
    _ <- step1.execute(())
   } yield ()
}
