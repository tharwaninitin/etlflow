package etlflow.task

import etlflow.audit
import etlflow.audit.Audit
import etlflow.http.{Http, HttpMethod}
import etlflow.log.ApplicationLogger
import sttp.client3.Response
import zio.test.Assertion.equalTo
import zio.test._
import zio.{RIO, TaskLayer, ULayer, ZIO}

object HttpTaskTestSuite extends ZIOSpecDefault with ApplicationLogger {

  override val bootstrap: ULayer[TestEnvironment] = testEnvironment ++ zioSlf4jLogger

  private val env: TaskLayer[Http with Audit] = Http.live ++ audit.noop

  private val getTask1: RIO[Http with Audit, Response[String]] = HttpRequestTask(
    name = "HttpGetSimple",
    url = "https://httpbin.org/get",
    method = HttpMethod.GET,
    connectionTimeout = 1200000
  ).toZIO

  private val getTask2: RIO[Http with Audit, Response[String]] = HttpRequestTask(
    name = "HttpGetParams",
    url = "https://httpbin.org/get",
    method = HttpMethod.GET,
    params = Right(Map("param1" -> "value1", "param2" -> "value2")),
    allowUnsafeSSL = true,
    log = true
  ).toZIO

  private val getTask3: RIO[Http with Audit, Response[String]] = HttpRequestTask(
    name = "HttpGetParams",
    url = "https://httpbin.org/get",
    method = HttpMethod.GET,
    params = Left("something"),
    allowUnsafeSSL = true,
    log = true
  ).toZIO

  private val postTask1: RIO[Http with Audit, Response[String]] = HttpRequestTask(
    name = "HttpPostJson",
    url = "https://httpbin.org/post",
    method = HttpMethod.POST,
    params = Left("""{"key":"value"}"""),
    headers = Map("X-Auth-Token" -> "abcd.xxx.123")
  ).toZIO

  private val postTask2: RIO[Http with Audit, Response[String]] = HttpRequestTask(
    name = "HttpPostForm",
    url = "https://httpbin.org/post?signup=yes",
    method = HttpMethod.POST,
    params = Right(Map("name" -> "John", "surname" -> "doe"))
  ).toZIO

  private val postTask3: RIO[Http with Audit, Response[String]] = HttpRequestTask(
    name = "HttpPostFormIncorrectHeader",
    url = "https://httpbin.org/post",
    method = HttpMethod.POST,
    params = Right(Map("param1" -> "value1")),
    headers = Map(
      "Content-Type" -> "application/json"
    ), // content-type header is ignored as we are sending Right(Map[String,String]) which encodes it as form
    log = true
  ).toZIO

  val putTask1: RIO[Http with Audit, Response[String]] = HttpRequestTask(
    name = "HttpPutJson",
    url = "https://httpbin.org/put",
    method = HttpMethod.PUT,
    params = Left("""{"key":"value"}"""),
    headers = Map("content-type" -> "application/json")
  ).toZIO

  val putTask2: RIO[Http with Audit, Response[String]] = HttpRequestTask(
    name = "HttpPutForm",
    url = "https://httpbin.org/put",
    method = HttpMethod.PUT,
    params = Right(Map("param1" -> "value1"))
  ).toZIO

  val job: RIO[Http with Audit, Unit] = for {
    _ <- getTask1
    _ <- getTask2
    _ <- getTask3.tapError(e => ZIO.succeed(logger.error(s"${e.getMessage}"))).ignore
    _ <- postTask1
    _ <- postTask2
    _ <- postTask3
    _ <- putTask1
    _ <- putTask2
  } yield ()

  override def spec: Spec[TestEnvironment, Any] =
    (suite("Http Tasks")(test("Execute Http tasks") {
      assertZIO(job.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    }) @@ TestAspect.flaky @@ TestAspect.retries(5)).provideShared(env.orDie)
}
