package etlflow.etltask

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

  private val step1 = SparkReadWriteTask[RatingBQ, RatingBQ](
    name = "LoadRatings BQ(table) to GCS CSV",
    inputLocation = List(datasetName + "." + tableName),
    inputType = BQ(),
    outputType = CSV(),
    outputLocation = ratingsIntermediateBucket,
    outputSaveMode = SaveMode.Overwrite,
    outputRepartitioning = true,
    outputRepartitioningNum = 3
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

  private val step21 = SparkReadWriteTask[Rating, RatingOutputCsv](
    name = "LoadRatings GCS Csv To GCS Csv",
    inputLocation = List(ratingsIntermediateBucket),
    inputType = CSV(),
    transformFunction = Some(enrichRatingCsvData),
    outputType = CSV(),
    outputLocation = ratingsOutputBucket1,
    outputSaveMode = SaveMode.Overwrite,
    outputRepartitioning = true,
    outputRepartitioningNum = 1,
    outputFilename = Some("ratings.csv")
  )

  def enrichRatingData(spark: SparkSession, in: Dataset[Rating]): Dataset[RatingOutput] = {
    import spark.implicits._

    val ratingsDf = in
      .withColumn("date", from_unixtime(col("timestamp"), "yyyy-MM-dd").cast(DateType))
      .withColumn(partitionDateCol, getFormattedDate("date", "yyyy-MM-dd", "yyyyMM").cast(IntegerType))
      .where(f"$partitionDateCol in ('201601', '201512', '201510')")

    ratingsDf.as[RatingOutput]
  }

  private val step22 = SparkReadWriteTask[Rating, RatingOutput](
    name = "LoadRatings GCS Csv To S3 Parquet",
    inputLocation = List(ratingsIntermediateBucket),
    inputType = CSV(),
    transformFunction = Some(enrichRatingData),
    outputType = PARQUET,
    outputLocation = ratingsOutputBucket2,
    outputSaveMode = SaveMode.Overwrite,
    outputPartitionCol = Seq(s"$partitionDateCol")
  )

  private val step3 = SparkReadWriteTask[Rating, RatingOutput](
    name = "LoadRatings GCS Csv To GCS Json",
    inputLocation = List(ratingsIntermediateBucket),
    inputType = CSV(),
    transformFunction = Some(enrichRatingData),
    outputType = JSON(),
    outputLocation = ratingsOutputBucket3,
    outputSaveMode = SaveMode.Overwrite,
    outputPartitionCol = Seq(s"$partitionDateCol"),
    outputRepartitioning = true,
    outputRepartitioningNum = 1
  )

  val job: RIO[SparkEnv with LogEnv, Unit] = for {
    _ <- step1.executeZio
    _ <- step21.executeZio.zipPar(step22.executeZio)
    _ <- step3.executeZio
  } yield ()

  val test: ZSpec[environment.TestEnvironment with SparkEnv with LogEnv, Any] =
    testM("Execute SparkReadWriteSteps with GCS and BQ") {
      assertM(job.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    }
}
