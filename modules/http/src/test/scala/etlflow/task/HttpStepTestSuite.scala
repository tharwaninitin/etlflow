package etlflow.task

import etlflow.http.HttpMethod
import etlflow.log.{noLog, LogEnv}
import etlflow.utils.ApplicationLogger
import zio.{RIO, ZIO}
import zio.test.Assertion.equalTo
import zio.test._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object HttpStepTestSuite extends DefaultRunnableSpec with ApplicationLogger {

  private val getStep1 = HttpRequestTask(
    name = "HttpGetSimple",
    url = "https://httpbin.org/get",
    method = HttpMethod.GET,
    connection_timeout = 1200000
  )

  private val getStep2 = HttpRequestTask(
    name = "HttpGetParams",
    url = "https://httpbin.org/get",
    method = HttpMethod.GET,
    params = Right(Map("param1" -> "value1", "param2" -> "value2"))
  )

  private val postStep1 = HttpRequestTask(
    name = "HttpPostJson",
    url = "https://httpbin.org/post",
    method = HttpMethod.POST,
    params = Left("""{"key":"value"}"""),
    headers = Map("X-Auth-Token" -> "abcd.xxx.123")
  )

  private val postStep2 = HttpRequestTask(
    name = "HttpPostForm",
    url = "https://httpbin.org/post?signup=yes",
    method = HttpMethod.POST,
    params = Right(Map("name" -> "John", "surname" -> "doe"))
  )

  private val postStep3 = HttpRequestTask(
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

  val putStep1: HttpRequestTask = HttpRequestTask(
    name = "HttpPutJson",
    url = "https://httpbin.org/put",
    method = HttpMethod.PUT,
    params = Left("""{"key":"value"}"""),
    headers = Map("content-type" -> "application/json")
  )

  val putStep2: HttpRequestTask = HttpRequestTask(
    name = "HttpPutForm",
    url = "https://httpbin.org/put",
    method = HttpMethod.PUT,
    params = Right(Map("param1" -> "value1"))
  )

  val job: RIO[LogEnv, Unit] = for {
    _ <- getStep1.executeZio
    _ <- getStep2.executeZio
    _ <- postStep1.executeZio
    _ <- postStep2.executeZio
    _ <- postStep3.executeZio
    _ <- putStep1.executeZio
    _ <- putStep2.executeZio
  } yield ()

  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    (suite("Http Steps")(testM("Execute Http steps") {
      assertM(job.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    }) @@ TestAspect.flaky).provideCustomLayerShared(noLog)
}
