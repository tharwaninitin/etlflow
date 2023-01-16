package etlflow.task

import etlflow.audit
import etlflow.audit.Audit
import etlflow.http.HttpMethod
import etlflow.log.ApplicationLogger
import zio.test.Assertion.equalTo
import zio.test._
import zio.{RIO, ZIO}

object HttpTaskTestSuite extends ZIOSpecDefault with ApplicationLogger {

  private val getTask1 = HttpRequestTask(
    name = "HttpGetSimple",
    url = "https://httpbin.org/get",
    method = HttpMethod.GET,
    connectionTimeout = 1200000
  )

  private val getTask2 = HttpRequestTask(
    name = "HttpGetParams",
    url = "https://httpbin.org/get",
    method = HttpMethod.GET,
    params = Right(Map("param1" -> "value1", "param2" -> "value2")),
    allowUnsafeSSL = true,
    log = true
  )

  private val getTask3 = HttpRequestTask(
    name = "HttpGetParams",
    url = "https://httpbin.org/get",
    method = HttpMethod.GET,
    params = Left("something"),
    allowUnsafeSSL = true,
    log = true
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
    name = "HttpPostFormIncorrectHeader",
    url = "https://httpbin.org/post",
    method = HttpMethod.POST,
    params = Right(Map("param1" -> "value1")),
    headers = Map(
      "Content-Type" -> "application/json"
    ), // content-type header is ignored as we are sending Right(Map[String,String]) which encodes it as form
    log = true
  )

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

  val job: RIO[Audit, Unit] = for {
    _ <- getTask1.toZIO
    _ <- getTask2.toZIO
    _ <- getTask3.toZIO.tapError(e => ZIO.succeed(logger.error(s"${e.getMessage}"))).ignore
    _ <- postTask1.toZIO
    _ <- postTask2.toZIO
    _ <- postTask3.toZIO
    _ <- putTask1.toZIO
    _ <- putTask2.toZIO
  } yield ()

  override def spec: Spec[TestEnvironment, Any] =
    (suite("Http Tasks")(test("Execute Http tasks") {
      assertZIO(job.foldZIO(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    }) @@ TestAspect.flaky).provideShared(audit.noop)
}
