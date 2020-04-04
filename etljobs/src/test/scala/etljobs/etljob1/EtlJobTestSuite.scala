package etljobs.etljob1

import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.sql.{Dataset, Row}
import etljobs.spark.{ReadApi, SparkManager}
import etljobs.utils.{AppLogger, CSV, GlobalProperties, ORC, PARQUET}
import etljobs.bigquery.{BigQueryManager, QueryApi}
import etljobs.schema.MyEtlJobList.EtlJob1PARQUETtoORCtoBQLocalWith2Steps
import etljobs.schema.MyEtlJobProps.EtlJob1Props
import etljobs.schema.EtlJobSchemas.RatingOutput

class EtlJobTestSuite extends FlatSpec with Matchers {
  // AppLogger.initialize()
  // STEP 1: Initialize job properties and create BQ tables required for jobs 
  val canonical_path: String = new java.io.File(".").getCanonicalPath
  val global_props: GlobalProperties = new GlobalProperties(canonical_path + "/etljobs/src/test/resources/loaddata.properties") {}

  val job_props = EtlJob1Props(
    ratings_input_path = List(s"$canonical_path/etljobs/src/test/resources/input/movies/ratings_parquet/*"),
    ratings_output_path = s"$canonical_path/etljobs/src/test/resources/output/movies/ratings",
    ratings_output_dataset = "test",
    ratings_output_table_name = "ratings",
    ratings_output_file_name = Some("ratings.orc")
  )
  job_props.job_notification_level = "info"

  // STEP 2: Execute JOB
  val etljob = new EtlJobDefinition(EtlJob1PARQUETtoORCtoBQLocalWith2Steps.toString,job_properties=job_props, global_properties=Some(global_props))
  etljob.execute

  // Could use Hlist here for getting single step out of job
  // To get errors in CSV if any, run single step like this
  // etljob.apply().filter(etl => etl.name == "LoadRatingsParquet").foreach{ etl =>
  //    etl.asInstanceOf[SparkReadWriteStep[Rating , Unit, RatingOutput, Unit]].showCorruptedData()
  //  }

  // STEP 3: Run tests
  val sm = new SparkManager {
    val global_properties: Option[GlobalProperties] = Some(global_props)
  }

  val raw: Dataset[RatingOutput] = ReadApi.LoadDS[RatingOutput](job_props.ratings_input_path, PARQUET)(sm.spark)
  val Row(sum_ratings: Double, count_ratings: Long) = raw.selectExpr("sum(rating)","count(*)").first()

  val destination_dataset = job_props.ratings_output_dataset
  val destination_table = job_props.ratings_output_table_name

  val bqm = new BigQueryManager {
    val global_properties: Option[GlobalProperties] = Some(global_props)
  }
  val query: String = s""" SELECT count(*) as count,sum(rating) sum_ratings 
                          FROM $destination_dataset.$destination_table """.stripMargin
  val result = QueryApi.getDataFromBQ(bqm.bq, query)
  val count_records_bq: Long = result.head.get("count").getLongValue
  val sum_ratings_bq: Double = result.head.get("sum_ratings").getDoubleValue

  "Record counts" should "be matching in transformed DF and BQ table " in {
    assert(count_ratings==count_records_bq)
  }

  "Sum of ratings" should "be matching in transformed DF and BQ table " in {
    assert(sum_ratings==sum_ratings_bq)
  }

}


