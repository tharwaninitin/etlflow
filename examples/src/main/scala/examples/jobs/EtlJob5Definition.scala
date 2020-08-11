package examples.jobs

import etlflow.EtlStepList
import etlflow.etljobs.SequentialEtlJob
import etlflow.etlsteps.SparkReadWriteStep
import etlflow.spark.SparkManager
import etlflow.utils.{BQ, Config, PARQUET}
import examples.schema.MyEtlJobProps
import examples.schema.MyEtlJobProps.EtlJob5Props
import examples.schema.MyEtlJobSchema.{Rating, RatingBQ}
import org.apache.spark.sql.{SaveMode, SparkSession}

case class EtlJob5Definition(job_properties: MyEtlJobProps, globalProperties: Config)
  extends SequentialEtlJob {
  private val global_props = globalProperties
  private val job_props = job_properties.asInstanceOf[EtlJob5Props]
  private implicit val spark: SparkSession = SparkManager.createSparkSession()
  private val step1 = SparkReadWriteStep[Rating](
    name             = "LoadRatingsParquetToJdbc",
    input_location   = job_props.ratings_input_path,
    input_type       = PARQUET,
    output_type      = global_props.dbLog,
    output_location  = job_props.ratings_output_table,
    output_save_mode = SaveMode.Overwrite
  )

  private val step2 = SparkReadWriteStep[RatingBQ](
    name             = "LoadRatingsBqToJdbc",
    input_location   = Seq("test.ratings"),
    input_type       = BQ,
    output_type      = global_props.dbLog,
    output_location  = job_props.ratings_output_table,
    output_save_mode = SaveMode.Overwrite
  )

  val etlStepList = EtlStepList(step1,step2)
}
