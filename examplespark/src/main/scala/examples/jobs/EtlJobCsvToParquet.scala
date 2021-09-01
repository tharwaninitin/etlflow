package examples.jobs

import etlflow.EtlStepList
import etlflow.etljobs.SequentialEtlJob
import etlflow.etlsteps.{SparkETLStep, SparkReadTransformWriteStep}
import etlflow.spark.Environment.{GCP, LOCAL}
import etlflow.spark.{IOType, ReadApi, SparkManager}
import examples.schema.MyEtlJobProps.EtlJob2Props
import examples.schema.MyEtlJobSchema.{Rating, RatingOutput}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, from_unixtime, input_file_name, unix_timestamp}
import org.apache.spark.sql.types.DateType

case class EtlJobCsvToParquet(job_properties: EtlJob2Props) extends SequentialEtlJob[EtlJob2Props]  {

  private val gcs_output_path = f"gs://${sys.env("GCS_BUCKET")}/output/ratings1"
  private var output_date_paths : Seq[(String,String)] = Seq()
  private val temp_date_col = "temp_date_col"
  private val job_props:EtlJob2Props  = job_properties

  private implicit val spark: SparkSession = SparkManager.createSparkSession(Set(LOCAL,GCP(sys.env("GOOGLE_APPLICATION_CREDENTIALS"), sys.env("GCP_PROJECT_ID"))), hive_support = false)

  val get_formatted_date: (String,String,String) => Column
  = (ColumnName:String,ExistingFormat:String,NewFormat:String) => {
    from_unixtime(unix_timestamp(col(ColumnName), ExistingFormat), NewFormat)
  }

  def enrichRatingData(spark: SparkSession, in : Dataset[Rating]) : Dataset[RatingOutput] = {
    val mapping = Encoders.product[RatingOutput]

    val ratings_df = in
        .withColumn("date", from_unixtime(col("timestamp"), "yyyy-MM-dd").cast(DateType))
        .withColumn(temp_date_col, get_formatted_date("date","yyyy-MM-dd","yyyyMMdd"))
        .where(f"$temp_date_col in ('20160101', '20160102')")

    ratings_df.as[RatingOutput](mapping)
  }

  def addFilePaths()(spark: SparkSession, ip: Unit): Unit = {
    import spark.implicits._
    output_date_paths = ReadApi.LoadDS[RatingOutput](Seq(gcs_output_path),IOType.PARQUET)(spark)
      .select(f"$temp_date_col")
      .withColumn("filename", input_file_name)
      .distinct()
      .as[(String,String)]
      .collect()
      .map((date) => (gcs_output_path + f"/$temp_date_col=" + date._1 + "/" + date._2.split("/").last, date._1))

    logger.info("Filepaths generated are: ")
    output_date_paths.foreach(path => println(path))
  }

  private val step1 = SparkReadTransformWriteStep[Rating, RatingOutput](
    name                  = "LoadRatingsParquet",
    input_location        = Seq(job_props.ratings_input_path),
    input_type            = IOType.CSV(",", true, "FAILFAST"),
    transform_function    = enrichRatingData,
    output_type           = IOType.PARQUET,
    output_location       = gcs_output_path,
    output_save_mode      = SaveMode.Overwrite,
    output_partition_col  = Seq(f"$temp_date_col"),
    output_repartitioning = true
  )

  private val step2 = SparkETLStep(
    name               = "GenerateFilePaths",
    transform_function = addFilePaths()
  )

  val etlStepList = EtlStepList(step1, step2)
}
