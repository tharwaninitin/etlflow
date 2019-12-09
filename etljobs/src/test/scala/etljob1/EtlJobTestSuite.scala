package etljob1

import org.scalatest.{FlatSpec, Matchers}

class EtlJobTestSuite extends FlatSpec with Matchers {

  val canonical_path = new java.io.File(".").getCanonicalPath

  val props : Map[String,String] = Map(
    "job_name" -> "EtlJobMovieRatings",
    "ratings_input_path" -> s"$canonical_path/etljobs/src/test/resources/input/movies/ratings/*",
    "ratings_output_path" -> s"$canonical_path/etljobs/src/test/resources/output/movies/ratings",
    "ratings_output_dataset" -> "test1",
    "ratings_output_table_name" -> "ratings",
    "ratings_output_file_name" -> "ratings.parquet"
    //"parse_mode" -> "PERMISSIVE"
  )

  val etljob = new EtlJobDefinition(props)
  etljob.getJobInfo().foreach(println(_))
  etljob.execute(props)

  // can use Hlist here for easy management
  //  EtlJobDefinition(job_properties)(spark_test).filter(etl => etl.name == "LoadRatings").foreach{ etl =>
  //    etl.asInstanceOf[SparkWriteEtlStepTyped[Rating]].showCorruptedData()
  //  }

  // println("Here 0")
  // EtlJobDefinition.printJobInfo(job_properties)(spark_test,bq_test)
  // EtlJobDefinition.execute(job_properties)

  //EtlJobDefinition.execute(job_properties)(spark_test)

//  val raw : Dataset[Rating]                         = LoadDS[Rating](Seq(job_properties("ratings_input_path")), CSV())
//  val transformed : Dataset[Row]                    = EtlJobDefinition.enrichRatingData(spark_test,job_properties)(raw)
//  val Row(sum_ratings: Double, count_ratings: Long) = transformed.selectExpr("sum(rating)","count(*)").first()
//
//  val destination_dataset = job_properties("ratings_output_dataset")
//  val destination_table = job_properties("ratings_output_table_name")
//
//  "Record counts" should "be matching in orc dataframe and BQ table " in {
//    val result_bq_record_count = s"""bq query --use_legacy_sql=false select count(*) from $destination_dataset.$destination_table """.!!
//    val result_bq_record_count_modified  = result_bq_record_count.replaceAll("\n|-|\\+|\\|| |_","f")
//    val bq_result_pattern = """[^\d]*[\d]+[^\d]+([\d]+)[^\d]*""".r
//    val bq_result_pattern(count_df_bq_str) = result_bq_record_count_modified
//    val count_records_df_bq = count_df_bq_str.toInt
//    assert(count_records_df_bq==count_ratings)
//  }
}


