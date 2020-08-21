package etlflow.utils

import etlflow.Schema._
import org.scalatest.{FlatSpec, Matchers}

class JsonTestSuite extends FlatSpec with Matchers {

  val httpBinJson = """{
    "args": {
      "param1": "value1"
    },
    "headers": {
      "Accept": "text/html, image/gif, image/jpeg, *; q=.2, */*; q=.2",
      "Accept-Encoding": "gzip,deflate",
      "Host": "httpbin.org"
    },
    "origin": "XX.XX.XX.XX",
    "url": "https://httpbin.org/get?param1=value1"
  }"""

  val student1Json: String = """{
                               |"name":"John",
                               |"id":63,
                               |"class": "101"
                               |}""".stripMargin

  val student2Json: String ="""{
                              |"name":"John",
                              |"id":63
                              |}""".stripMargin

  val httpBin: HttpBinResponse = JsonJackson.convertToObject[HttpBinResponse](httpBinJson)
  val student1: Student = JsonJackson.convertToObject[Student](student1Json)
  val student2: Student = JsonJackson.convertToObject[Student](student2Json)

  val excludeKeys = List("job_run_id","job_description","job_properties","job_aggregate_error")

  //Input data for all logging level and deploy mode.
  val inputDebugLevel       = EtlJob23Props("data/movies/ratings/*","test","ratings_par",true,LoggingLevel.DEBUG, job_deploy_mode = Executor.KUBERNETES("etlflow","default",Map.empty,None,None,None))
  val inputInfoLevel        = EtlJob23Props("data/movies/ratings/*","test","ratings_par",true,LoggingLevel.INFO,job_deploy_mode = Executor.KUBERNETES("etlflow","default",Map.empty,None,None,None))
  val inputJobInput         = EtlJob23Props("data/movies/ratings/*","test","ratings_par",true,LoggingLevel.JOB,job_deploy_mode = Executor.DATAPROC("","","",""))
  val inputJobNegativeInput = EtlJob23Props("data/movies/ratings/*","test","ratings_par",true,LoggingLevel.JOB,job_deploy_mode = Executor.LOCAL)


  //Output data for all logging level and deploy mode.
  val outputDebugLevel = Map("job_send_slack_notification" -> true,
    "job_enable_db_logging" -> true,
    "job_notification_level" -> "debug",
    "ratings_output_table_name" -> "ratings_par",
    "ratings_input_path" -> "data/movies/ratings/*",
    "ratings_output_dataset" -> "test",
    "job_deploy_mode" -> "kubernetes",
    "job_schedule" -> ""
  )
  val outputInfoLevel = Map(
    "job_send_slack_notification" -> true,
    "job_enable_db_logging" -> true,
    "job_notification_level" -> "info",
    "ratings_output_table_name" -> "ratings_par",
    "ratings_input_path" -> "data/movies/ratings/*",
    "ratings_output_dataset" -> "test",
    "job_deploy_mode" -> "kubernetes",
    "job_schedule" -> ""
  )
  val outputJobLevel = Map("job_send_slack_notification" -> true,
    "job_enable_db_logging" -> true,
    "job_notification_level" -> "job",
    "ratings_output_table_name" -> "ratings_par",
    "ratings_input_path" -> "data/movies/ratings/*",
    "ratings_output_dataset" -> "test",
    "job_deploy_mode" -> "dataproc",
    "job_schedule" -> ""
  )
  val outputJobLevelNegative = Map("job_send_slack_notification" -> true,
    "job_enable_db_logging" -> true,
    "job_notification_level" -> "info",
    "ratings_output_table_name" -> "ratings_par",
    "ratings_input_path" -> "data/movies/ratings/*",
    "ratings_output_dataset" -> "test",
    "job_deploy_mode" -> "local",
    "job_schedule" -> ""
  )


  val expectedserializerOutput = """{
                                   |  "ratings_input_path" : "data/movies/ratings/*",
                                   |  "ratings_output_dataset" : "test",
                                   |  "ratings_output_table_name" : "ratings_par",
                                   |  "job_send_slack_notification" : true,
                                   |  "job_notification_level" : "debug",
                                   |  "job_deploy_mode" : "kubernetes",
                                   |  "job_enable_db_logging" : true,
                                   |  "job_schedule" : ""
                                   |}""".stripMargin

  val expectedserializerNegativeOutput = """{
                             "ratings_input_path" : "data/movies/ratings/*",
                                           |  "ratings_output_dataset" : "test",
                                           |  "ratings_output_table_name" : "ratings_par",
                                           |  "job_send_slack_notification" : true,
                                           |  "job_notification_level" : "debug",
                                           |  "job_deploy_mode" : "local",
                                           |  "job_enable_db_logging" : true,
                                           |  "job_schedule" : ""
                                           |}""".stripMargin


  val actualOutputDebugLevel       = JsonJackson.convertToJsonByRemovingKeysAsMap(inputDebugLevel,excludeKeys)
  val actualOutputInfoLevel        = JsonJackson.convertToJsonByRemovingKeysAsMap(inputInfoLevel,excludeKeys)
  val actualOutputJobLevel         = JsonJackson.convertToJsonByRemovingKeysAsMap(inputJobInput,excludeKeys)
  val actualOutputJobLevelNegative = JsonJackson.convertToJsonByRemovingKeysAsMap(inputJobNegativeInput,excludeKeys)

  val actualSerializerInput = JsonJackson.convertToJsonByRemovingKeys(inputDebugLevel,excludeKeys)

  "Json Deserializer : ConvertToObject  Student1 " should "run successfully" in {
    assert(student1 == Student("63","John",Some("101")))
  }

  "Json Deserializer : ConvertToObject  Student2" should "run successfully" in {
    assert(student2 == Student("63","John",None))
  }

  "Json Deserializer : ConvertToJsonByRemovingKeysAsMap debug,kubenetes" should "run successfully" in {
    assert(actualOutputDebugLevel == outputDebugLevel)
  }

  "Json Deserializer : ConvertToJsonByRemovingKeysAsMap info,kubenetes" should "run successfully" in {
    assert(actualOutputInfoLevel == outputInfoLevel)
  }

  "Json Deserializer : ConvertToJsonByRemovingKeysAsMap job,dataproc " should "run successfully" in {
    assert(actualOutputJobLevel == outputJobLevel)
  }

  //Input has job_notification_level=info and output has job. Test case pass on not matching input and output.
  "Json Deserializer : ConvertToJsonByRemovingKeysAsMap job,local" should "run successfully" in {
    assert(actualOutputJobLevelNegative != outputJobLevelNegative)
  }

  "Json Serializer : ConvertToJsonByRemovingKeys debug,kubenetes" should "run successfully" in {
    assert(actualSerializerInput == expectedserializerOutput)
  }

  //Input has job_notification_level=info and output has "job" notification level. Test case pass on not matching input and output.
  "Json Serializer: ConvertToJsonByRemovingKeys debug,kubenetes negative scenario" should "run successfully" in {
    assert(actualSerializerInput != expectedserializerNegativeOutput)
  }

}