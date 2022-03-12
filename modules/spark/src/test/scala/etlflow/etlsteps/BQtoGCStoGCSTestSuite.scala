package etlflow.etlsteps

import etlflow.SparkTestSuiteHelper
import etlflow.log.LogEnv
import etlflow.schema.{Rating, RatingBQ, RatingOutput, RatingOutputCsv}
import etlflow.spark.IOType.{BQ, CSV, JSON, PARQUET}
import etlflow.spark.{SparkEnv, SparkUDF}
import org.apache.spark.sql.functions.{col, from_unixtime}
import org.apache.spark.sql.types.{DateType, IntegerType}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import zio._
import zio.test.Assertion.equalTo
import zio.test._

object BQtoGCStoGCSTestSuite extends SparkUDF with SparkTestSuiteHelper {

  val partitionDateCol = "date_int"

//  private val query = s""" SELECT * FROM $datasetName.$tableName` """.stripMargin
//  private val step0 = SparkReadWriteStep[Rating, Rating](
//    name = "LoadRatings BQ(query) to GCS CSV",
//    input_location = List(query),
//    input_type = BQ(temp_dataset = datasetName, operation_type = "query"),
//    output_type = CSV(),
//    output_location = ratingsIntermediateBucket,
//    output_save_mode = SaveMode.Overwrite,
//    output_repartitioning = true,
//    output_repartitioning_num = 3
//  )

  private val step1 = SparkReadWriteStep[RatingBQ, RatingBQ](
    name = "LoadRatings BQ(table) to GCS CSV",
    input_location = List(datasetName + "." + tableName),
    input_type = BQ(),
    output_type = CSV(),
    output_location = ratingsIntermediateBucket,
    output_save_mode = SaveMode.Overwrite,
    output_repartitioning = true,
    output_repartitioning_num = 3
  )

  def enrichRatingCsvData(spark: SparkSession, in: Dataset[Rating]): Dataset[RatingOutputCsv] = {
    import spark.implicits._

    // val mapping = Encoders.product[RatingOutputCsv]

    val ratingsDf = in
      .withColumnRenamed("user_id", "User Id")
      .withColumnRenamed("movie_id", "Movie Id")
      .withColumnRenamed("rating", "Ratings")
      .withColumn("date", from_unixtime(col("timestamp"), "yyyy-MM-dd").cast(DateType))
      .withColumnRenamed("date", "Movie Date")

    ratingsDf.as[RatingOutputCsv]
  }

  private val step21 = SparkReadWriteStep[Rating, RatingOutputCsv](
    name = "LoadRatings GCS Csv To GCS Csv",
    input_location = List(ratingsIntermediateBucket),
    input_type = CSV(),
    transform_function = Some(enrichRatingCsvData),
    output_type = CSV(),
    output_location = ratingsOutputBucket1,
    output_save_mode = SaveMode.Overwrite,
    output_repartitioning = true,
    output_repartitioning_num = 1,
    output_filename = Some("ratings.csv")
  )

  def enrichRatingData(spark: SparkSession, in: Dataset[Rating]): Dataset[RatingOutput] = {
    import spark.implicits._

    val ratingsDf = in
      .withColumn("date", from_unixtime(col("timestamp"), "yyyy-MM-dd").cast(DateType))
      .withColumn(partitionDateCol, getFormattedDate("date", "yyyy-MM-dd", "yyyyMM").cast(IntegerType))
      .where(f"$partitionDateCol in ('201601', '201512', '201510')")

    ratingsDf.as[RatingOutput]
  }

  private val step22 = SparkReadWriteStep[Rating, RatingOutput](
    name = "LoadRatings GCS Csv To S3 Parquet",
    input_location = List(ratingsIntermediateBucket),
    input_type = CSV(),
    transform_function = Some(enrichRatingData),
    output_type = PARQUET,
    output_location = ratingsOutputBucket2,
    output_save_mode = SaveMode.Overwrite,
    output_partition_col = Seq(s"$partitionDateCol")
  )

  private val step3 = SparkReadWriteStep[Rating, RatingOutput](
    name = "LoadRatings GCS Csv To GCS Json",
    input_location = List(ratingsIntermediateBucket),
    input_type = CSV(),
    transform_function = Some(enrichRatingData),
    output_type = JSON(),
    output_location = ratingsOutputBucket3,
    output_save_mode = SaveMode.Overwrite,
    output_partition_col = Seq(s"$partitionDateCol"),
    output_repartitioning = true,
    output_repartitioning_num = 1
  )

  val job: RIO[SparkEnv with LogEnv, Unit] = for {
    _ <- step1.execute
    _ <- step21.execute.zipPar(step22.execute)
    _ <- step3.execute
  } yield ()

  val test: ZSpec[environment.TestEnvironment with SparkEnv with LogEnv, Any] =
    testM("Execute SparkReadWriteSteps with GCS and BQ") {
      assertM(job.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    }
}
