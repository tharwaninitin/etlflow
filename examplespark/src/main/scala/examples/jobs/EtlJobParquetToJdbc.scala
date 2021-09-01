package examples.jobs

import etlflow.EtlStepList
import etlflow.etljobs.SequentialEtlJob
import etlflow.etlsteps.SparkReadWriteStep
import etlflow.schema.Credential.JDBC
import etlflow.spark.IOType.RDB
import etlflow.spark.{IOType, SparkManager}
import etlflow.utils.Configuration
import examples.schema.MyEtlJobProps.EtlJob4Props
import examples.schema.MyEtlJobSchema.{Rating, RatingBQ}
import org.apache.spark.sql.{SaveMode, SparkSession}

case class EtlJobParquetToJdbc(job_properties: EtlJob4Props) extends SequentialEtlJob[EtlJob4Props] {

  private implicit val spark: SparkSession = SparkManager.createSparkSession(Set(etlflow.spark.Environment.LOCAL),hive_support = false)

  private val step1 = SparkReadWriteStep[Rating](
    name             = "LoadRatingsParquetToJdbc",
    input_location   = job_properties.ratings_input_path,
    input_type       = IOType.PARQUET,
    output_type      = RDB(JDBC(sys.env("DB_URL"),sys.env("DB_USER"),sys.env("DB_PWD"),sys.env("DB_DRIVER"))),
    output_location  = job_properties.ratings_output_table,
    output_save_mode = SaveMode.Overwrite
  )

  private val step2 = SparkReadWriteStep[RatingBQ](
    name             = "LoadRatingsBqToJdbc",
    input_location   = Seq("dev.ratings"),
    input_type       = IOType.BQ(),
    output_type      = RDB(JDBC(sys.env("DB_URL"),sys.env("DB_USER"),sys.env("DB_PWD"),sys.env("DB_DRIVER"))),
    output_location  = job_properties.ratings_output_table,
    output_save_mode = SaveMode.Overwrite
  )

  val etlStepList = EtlStepList(step1, step2)
}
