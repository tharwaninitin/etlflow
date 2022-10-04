package etlflow.task

import etlflow.http.HttpMethod
import etlflow.audit.{noLog, LogEnv}
import etlflow.utils.ApplicationLogger
import zio.{RIO, ZIO}
import zio.test.Assertion.equalTo
import zio.test._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import zio.test.ZIOSpecDefault

object HttpTaskTestSuite extends ZIOSpecDefault with ApplicationLogger {

  private val getTask1 = HttpRequestTask(
    name = "HttpGetSimple",
    url = "https://httpbin.org/get",
    method = HttpMethod.GET,
    connection_timeout = 1200000
  )

  private val getTask2 = HttpRequestTask(
    name = "HttpGetParams",
    url = "https://httpbin.org/get",
    method = HttpMethod.GET,
    params = Right(Map("param1" -> "value1", "param2" -> "value2"))
  )

  private val postTask1 = HttpRequestTask(
    name = "HttpPostJson",
    url = "https://httpbin.org/post",
    method = HttpMethod.POST,
    params = Left("""{"key":"value"}"""),
    headers = Map("X-Auth-Token" -> "abcd.xxx.123")
  )

  private val postTask2 = HttpRequestTask(
    name = "HttpPostForm",
    url = "https://httpbin.org/post?signup=yes",
    method = HttpMethod.POST,
    params = Right(Map("name" -> "John", "surname" -> "doe"))
  )

  private val postTask3 = HttpRequestTask(
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

  val putTask1: HttpRequestTask = HttpRequestTask(
    name = "HttpPutJson",
    url = "https://httpbin.org/put",
    method = HttpMethod.PUT,
    params = Left("""{"key":"value"}"""),
    headers = Map("content-type" -> "application/json")
  )

  val putTask2: HttpRequestTask = HttpRequestTask(
    name = "HttpPutForm",
    url = "https://httpbin.org/put",
    method = HttpMethod.PUT,
    params = Right(Map("param1" -> "value1"))
  )

  val job: RIO[LogEnv, Unit] = for {
    _ <- getTask1.execute
    _ <- getTask2.execute
    _ <- postTask1.execute
    _ <- postTask2.execute
    _ <- postTask3.execute
    _ <- putTask1.execute
    _ <- putTask2.execute
  } yield ()

  override def spec: Spec[TestEnvironment, Any] =
    (suite("Http Tasks")(test("Execute Http tasks") {
      assertZIO(job.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    }) @@ TestAspect.flaky).provideShared(noLog)
}
