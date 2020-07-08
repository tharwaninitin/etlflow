package etlflow.jobs

import org.scalatest.{FlatSpec, Matchers}
import etlflow.Schema._
import etlflow.utils.JsonJackson

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

  "Json Deserializer Student1" should "run successfully" in {
    assert(student1 == Student("63","John",Some("101")))
  }

  "Json Deserializer Student2(with Option field)" should "run successfully" in {
    assert(student2 == Student("63","John",None))
  }
}