package etlflow.etlsteps

import etlflow.http.HttpMethod
import etlflow.utils.ApplicationLogger
import zio.{Task, ZIO}
import zio.test.Assertion.equalTo
import zio.test._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object HttpStepTestSuite extends DefaultRunnableSpec with ApplicationLogger {

  val getStep1 = HttpRequestStep(
    name = "HttpGetSimple",
    url = "https://httpbin.org/get",
    method = HttpMethod.GET,
    connection_timeout = 1200000
  )

  val getStep2 = HttpRequestStep(
    name = "HttpGetParams",
    url = "https://httpbin.org/get",
    method = HttpMethod.GET,
    params = Right(Map("param1" -> "value1", "param2" -> "value2"))
  )

  val postStep1 = HttpRequestStep(
    name = "HttpPostJson",
    url = "https://httpbin.org/post",
    method = HttpMethod.POST,
    params = Left("""{"key":"value"}"""),
    headers = Map("X-Auth-Token" -> "abcd.xxx.123")
  )

  val postStep2 = HttpRequestStep(
    name = "HttpPostForm",
    url = "https://httpbin.org/post?signup=yes",
    method = HttpMethod.POST,
    params = Right(Map("name" -> "John", "surname" -> "doe"))
  )

  val postStep3 = HttpRequestStep(
    name = "HttpPostJsonParamsIncorrect",
    url = "https://httpbin.org/post",
    method = HttpMethod.POST,
    params = Right(Map("param1" -> "value1")),
    headers = Map("Content-Type" -> "application/json") // content-type header is ignored
  )

  val emailBody: String = {
    val exec_time = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm").format(LocalDateTime.now)
    s"""
       | SMTP Email Test
       | Time of Execution: $exec_time
       |""".stripMargin
  }

  val putStep1: HttpRequestStep = HttpRequestStep(
    name = "HttpPutJson",
    url = "https://httpbin.org/put",
    method = HttpMethod.PUT,
    params = Left("""{"key":"value"}"""),
    headers = Map("content-type" -> "application/json")
  )

  val putStep2: HttpRequestStep = HttpRequestStep(
    name = "HttpPutForm",
    url = "https://httpbin.org/put",
    method = HttpMethod.PUT,
    params = Right(Map("param1" -> "value1"))
  )

  val job: Task[Unit] = for {
    _ <- getStep1.process
    _ <- getStep2.process
    _ <- postStep1.process
    _ <- postStep2.process
    _ <- postStep3.process
    _ <- putStep1.process
    _ <- putStep2.process
  } yield ()

  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("Http Steps")(testM("Execute Http steps") {
      assertM(job.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    }) @@ TestAspect.flaky
}
