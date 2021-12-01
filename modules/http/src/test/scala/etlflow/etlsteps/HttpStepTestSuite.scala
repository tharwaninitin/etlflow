package etlflow.etlsteps

import etlflow.coretests.Schema.HttpBinResponse
import etlflow.coretests.TestSuiteHelper
import etlflow.http.HttpMethod
import etlflow.utils.ApplicationLogger
import io.circe.generic.auto._
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test.{DefaultRunnableSpec, ZSpec, assertM}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object HttpStepTestSuite extends DefaultRunnableSpec with TestSuiteHelper with ApplicationLogger {

  val getStep1 = HttpRequestStep[Unit](
    name = "HttpGetSimple",
    url = "https://httpbin.org/get",
    method = HttpMethod.GET,
    log = true,
    connection_timeout = 1200000
  )

  val getStep2 = HttpRequestStep[String](
    name = "HttpGetParams",
    url = "https://httpbin.org/get",
    method = HttpMethod.GET,
    params = Right(Map("param1" -> "value1")),
    log = true,
  )

  val getStep3 = HttpRequestStep[HttpBinResponse](
    name = "HttpGetParamsParsedResponse",
    url = "https://httpbin.org/get",
    method = HttpMethod.GET,
    params = Right(Map("param1" -> "value1", "param2" -> "value2")),
    log = true,
  )

  val postStep1 = HttpRequestStep[Unit](
    name = "HttpPostJson",
    url = "https://httpbin.org/post",
    method = HttpMethod.POST,
    params = Left("""{"key":"value"}"""),
    headers = Map("X-Auth-Token" -> "abcd.xxx.123"),
    log = true,
  )

  val postStep2 = HttpRequestStep[String](
    name = "HttpPostForm",
    url = "https://httpbin.org/post?signup=yes",
    method = HttpMethod.POST,
    params = Right(Map("name" -> "John", "surname" -> "doe")),
    log = true,
  )

  val postStep3 = HttpRequestStep[Unit](
    name = "HttpPostJsonParamsIncorrect",
    url = "https://httpbin.org/post",
    method = HttpMethod.POST,
    params = Right(Map("param1" -> "value1")),
    headers = Map("Content-Type" -> "application/json"), // content-type header is ignored
    log = true,
  )

  val emailBody: String = {
    val exec_time = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm").format(LocalDateTime.now)
    s"""
       | SMTP Email Test
       | Time of Execution: $exec_time
       |""".stripMargin
  }

  def processData(ip: HttpBinResponse): Unit = {
    logger.info("Processing Data")
    logger.info(ip.toString)
  }

  val genericStep = GenericETLStep(
    name = "ProcessData",
    transform_function = processData,
  )

  val putStep1 = HttpRequestStep[String](
    name = "HttpPutJson",
    url = "https://httpbin.org/put",
    method = HttpMethod.PUT,
    params = Left("""{"key":"value"}"""),
    headers = Map("content-type" -> "application/json"),
    log = true,
  )

  val putStep2 = HttpRequestStep[Unit](
    name = "HttpPutForm",
    url = "https://httpbin.org/put",
    method = HttpMethod.PUT,
    params = Right(Map("param1" -> "value1")),
    log = true,
  )

  val job = for {
    _ <- getStep1.process(())
    op2 <- getStep2.process(())
    op3 <- getStep3.process(())
    _ <- postStep1.process(())
    op2 <- postStep2.process(())
    _ <- postStep3.process(())
    _ <- genericStep.process(op3)
    _ <- putStep1.process(())
    _ <- putStep2.process(())
  } yield ()

  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("Http Steps")(
      testM("Execute Http steps") {
        assertM(job.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      })
}
