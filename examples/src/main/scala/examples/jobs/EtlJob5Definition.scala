package examples.jobs

import etlflow.EtlStepList
import etlflow.etljobs.SequentialEtlJob
import etlflow.etlsteps.SparkReadWriteStep
import etlflow.spark.SparkManager
import examples.schema.MyEtlJobProps.EtlJob5Props
import examples.schema.MyEtlJobSchema.{Rating, RatingBQ}
import org.apache.spark.sql.{SaveMode, SparkSession}
import etlflow.spark.IOType
import etlflow.spark.Environment.{GCP, LOCAL}
import etlflow.spark.IOType.JDBC

case class EtlJob5Definition(job_properties: EtlJob5Props) extends SequentialEtlJob[EtlJob5Props] {

  private implicit val spark: SparkSession = SparkManager.createSparkSession(Set(LOCAL),hive_support = false)

  private val step1 = SparkReadWriteStep[Rating](
    name             = "LoadRatingsParquetToJdbc",
    input_location   = job_properties.ratings_input_path,
    input_type       = IOType.PARQUET,
    output_type      = JDBC(config.dbLog.url,config.dbLog.user,config.dbLog.password,config.dbLog.driver),
    output_location  = job_properties.ratings_output_table,
    output_save_mode = SaveMode.Overwrite
  )

//  private val step2 = SparkReadWriteStep[RatingBQ](
//    name             = "LoadRatingsBqToJdbc",
//    input_location   = Seq("test.ratings"),
//    input_type       = IOType.BQ(),
//    output_type      = JDBC(config.dbLog.url,config.dbLog.user,config.dbLog.password,config.dbLog.driver),
//    output_location  = job_properties.ratings_output_table,
//    output_save_mode = SaveMode.Overwrite
//  )

  val etlStepList = EtlStepList(step1)
}
