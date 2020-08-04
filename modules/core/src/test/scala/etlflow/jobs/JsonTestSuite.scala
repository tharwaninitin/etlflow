package etlflow.jobs

import org.scalatest.{FlatSpec, Matchers}
import etlflow.Schema._
import etlflow.utils.{JsonJackson, LoggingLevel}

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

  val deSerializerExpectedOutput1 = Map("job_send_slack_notification" -> true, "job_enable_db_logging" -> true, "job_notification_level" -> "debug", "ratings_output_table_name" -> "ratings_par", "ratings_input_path" -> "data/movies/ratings/*", "ratings_output_dataset" -> "test", "job_deploy_mode" -> "local", "job_schedule" -> "")
  val serializerOutput1 = """{
                        |  "ratings_input_path" : "data/movies/ratings/*",
                        |  "ratings_output_dataset" : "test",
                        |  "ratings_output_table_name" : "ratings_par",
                        |  "job_send_slack_notification" : true,
                        |  "job_notification_level" : "debug",
                        |  "job_enable_db_logging" : true,
                        |  "job_schedule" : "",
                        |  "job_deploy_mode" : "local"
                        |}""".stripMargin

  val serializerOutput2 = """{
                        |  "ratings_input_path" : "data/movies/ratings/*",
                        |  "ratings_output_dataset" : "test",
                        |  "ratings_output_table_name" : "ratings_par",
                        |  "job_send_slack_notification" : true,
                        |  "job_notification_level" : "info",
                        |  "job_enable_db_logging" : true,
                        |  "job_schedule" : "",
                        |  "job_deploy_mode" : "local"
                        |}""".stripMargin

  val input1 = EtlJob23Props("data/movies/ratings/*","test","ratings_par",true,LoggingLevel.DEBUG)

  val deSerializerExpectedOutput2 = Map("job_send_slack_notification" -> true, "job_enable_db_logging" -> true, "job_notification_level" -> "info", "ratings_output_table_name" -> "ratings_par", "ratings_input_path" -> "data/movies/ratings/*", "ratings_output_dataset" -> "test", "job_deploy_mode" -> "local", "job_schedule" -> "")
  val input2 = EtlJob23Props("data/movies/ratings/*","test","ratings_par",true,LoggingLevel.INFO)

  val deSerializerExpectedOutput3 = Map("job_send_slack_notification" -> true, "job_enable_db_logging" -> true, "job_notification_level" -> "job", "ratings_output_table_name" -> "ratings_par", "ratings_input_path" -> "data/movies/ratings/*", "ratings_output_dataset" -> "test", "job_deploy_mode" -> "local", "job_schedule" -> "")
  val input3 = EtlJob23Props("data/movies/ratings/*","test","ratings_par",true,LoggingLevel.JOB)

  val deSerializerExpectedOutput4 = Map("job_send_slack_notification" -> true, "job_enable_db_logging" -> true, "job_notification_level" -> "info", "ratings_output_table_name" -> "ratings_par", "ratings_input_path" -> "data/movies/ratings/*", "ratings_output_dataset" -> "test", "job_deploy_mode" -> "local", "job_schedule" -> "")
  val input4 = EtlJob23Props("data/movies/ratings/*","test","ratings_par",true,LoggingLevel.JOB)

  val excludeKeys = List("job_run_id","job_description","job_properties","job_aggregate_error")

  val deSerializerOutput1 = JsonJackson.convertToJsonByRemovingKeysAsMap(input1,excludeKeys)
  val deSerializerOutput2 = JsonJackson.convertToJsonByRemovingKeysAsMap(input2,excludeKeys)
  val deSerializerOutput3 = JsonJackson.convertToJsonByRemovingKeysAsMap(input3,excludeKeys)
  val deSerializerOutput4 = JsonJackson.convertToJsonByRemovingKeysAsMap(input4,excludeKeys)

  val serializerInput = JsonJackson.convertToJsonByRemovingKeys(input1,excludeKeys)


  "Json Deserializer : ConvertToObject  Student1 " should "run successfully" in {
    assert(student1 == Student("63","John",Some("101")))
  }


  "Json Deserializer : ConvertToObject  Student2" should "run successfully" in {
    assert(student2 == Student("63","John",None))
  }

  "Json Deserializer : ConvertToJsonByRemovingKeysAsMap Test case 1" should "run successfully" in {
    assert(deSerializerOutput1 == deSerializerExpectedOutput1)
  }

  "Json Deserializer : ConvertToJsonByRemovingKeysAsMap Test case 2" should "run successfully" in {
    assert(deSerializerOutput2 == deSerializerExpectedOutput2)
  }

  "Json Deserializer : ConvertToJsonByRemovingKeysAsMap Test case 3" should "run successfully" in {
    assert(deSerializerOutput3 == deSerializerExpectedOutput3)
  }

  //Input has job_notification_level=info and output has job. Test case pass on not matching input and output.
  "Json Deserializer : ConvertToJsonByRemovingKeysAsMap Test case 4(Negative scenario)" should "run successfully" in {
    assert(deSerializerOutput4 != deSerializerExpectedOutput4)
  }

  "Json Serializer : ConvertToJsonByRemovingKeys Test case 1" should "run successfully" in {
    assert(serializerInput == serializerOutput1)
  }

  //Input has job_notification_level=info and output has "job" notification level. Test case pass on not matching input and output.
  "Json Serializer: ConvertToJsonByRemovingKeys Test case 2" should "run successfully" in {
    assert(serializerInput != serializerOutput2)
  }
}