package etlflow.jobs

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import etlflow.Schema.HttpBinResponse
import etlflow.etljobs.GenericEtlJob
import etlflow.etlsteps._
import etlflow.spark.SparkUDF
import etlflow.utils.{GlobalProperties, SMTP}
import etlflow.{EtlJobProps, LoggerResource}
import zio.ZIO

case class EtlJob3Definition(job_properties: EtlJobProps, global_properties: Option[GlobalProperties])
  extends GenericEtlJob with SparkUDF {

  val step1 = HttpStep(
    name         = "HttpGetSimple",
    url          = "https://httpbin.org/get",
    http_method  = HttpMethod.GET,
    log_response = true,
  )

  val step2 = HttpResponseStep(
    name         = "HttpGetParams",
    url          = "https://httpbin.org/get",
    http_method  = HttpMethod.GET,
    params       = Right(Seq(("param1","value1"))),
    log_response = true,
  )

  val step3 = HttpStep(
    name         = "HttpPostJson",
    url          = "https://httpbin.org/post",
    http_method  = HttpMethod.POST,
    params       = Left("""{"key":"value"}"""),
    headers      = Map("content-type"->"application/json"),
    log_response = true,
  )

  val step4 = HttpResponseStep(
    name         = "HttpPostParams",
    url          = "https://httpbin.org/post",
    http_method  = HttpMethod.POST,
    params       = Right(Seq(("param1","value1"))),
    log_response = true,
  )

  val emailBody: String = {
    val exec_time = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm").format(LocalDateTime.now)
    s"""
       | SMTP Email Test
       | Time of Execution: $exec_time
       |""".stripMargin
  }

  val step5 = HttpParsedResponseStep[HttpBinResponse](
    name         = "HttpGetParamsStep5",
    url          = "https://httpbin.org/get",
    http_method  = HttpMethod.GET,
    params       = Right(Seq(("param1","value1"))),
    log_response = true,
  )

  def processData(ip: HttpBinResponse): Unit = {
    etl_job_logger.info("Processing Data")
    etl_job_logger.info(ip.toString)
  }

  val step6 = GenericETLStep(
    name               = "ProcessData",
    transform_function = processData,
  )

  val step7 = SendMailStep(
    name           = "SendSMTPEmail",
    body           = emailBody,
    subject        = "EtlFlow Test Ran Successfully",
    recipient_list = List(sys.env.getOrElse("SMTP_RECIPIENT","...")),
    credentials    = SMTP(
      sys.env.getOrElse("SMTP_PORT","587"),
      sys.env.getOrElse("SMTP_HOST","..."),
      sys.env.getOrElse("SMTP_USER","..."),
      sys.env.getOrElse("SMTP_PASS","..."),
    )
  )

  val job: ZIO[LoggerResource, Throwable, Unit] = for {
//    _     <-  step1.execute()
//    op1   <-  step2.execute()
//    _     <-  step3.execute()
//    op3   <-  step4.execute()
    op4   <-  step5.execute()
    _     <-  step6.execute(op4)
  } yield ()
}