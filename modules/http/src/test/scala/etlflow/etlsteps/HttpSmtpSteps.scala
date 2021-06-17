package etlflow.etlsteps

import etlflow.coretests.Schema.{EtlJob3Props, HttpBinResponse}
import etlflow.etljobs.GenericEtlJob
import etlflow.etlsteps.{GenericETLStep, SendMailStep}
import etlflow.schema.Credential.SMTP
import etlflow.utils.HttpRequest.HttpMethod

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

case class HttpSmtpSteps(job_properties: EtlJob3Props) extends GenericEtlJob[EtlJob3Props] {

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

  val step10 = SendMailStep(
    name = "SendSMTPEmail",
    body = emailBody,
    subject = "EtlFlow Test Ran Successfully",
    recipient_list = List(sys.env.getOrElse("SMTP_RECIPIENT", "...")),
    credentials = SMTP(
      sys.env.getOrElse("SMTP_PORT", "587"),
      sys.env.getOrElse("SMTP_HOST", "..."),
      sys.env.getOrElse("SMTP_USER", "..."),
      sys.env.getOrElse("SMTP_PASS", "..."),
    )
  )

  val job = for {
    _ <- getStep1.execute()
    op2 <- getStep2.execute()
    op3 <- getStep3.execute()
    _ <- postStep1.execute()
    op2 <- postStep2.execute()
    _ <- postStep3.execute()
    _ <- genericStep.execute(op3)
    _ <- putStep1.execute()
    _ <- putStep2.execute()
  } yield ()
}
