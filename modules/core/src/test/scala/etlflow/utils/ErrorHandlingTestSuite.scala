package etlflow.utils

import zio.test.Assertion.equalTo
import zio.test._
import zio.{UIO, ZIO}

object ErrorHandlingTestSuite {

  def funcArithmeticEx: Int            = 3 / 0
  def funcArrayIndexOutOfBoundsEx: Int = Array(0, 1, 2, 3, 4)(9)
  def funcFatalError: Int              = throw new StackOverflowError()

  val spec: ZSpec[environment.TestEnvironment, Any] =
    suite("Error Handling")(
      test("logTry 1") {
        val logTry = LogTry(funcArithmeticEx)
        assertTrue(logTry.isFailure)
      },
      test("logTry 2") {
        val logTry = LogTry(funcArrayIndexOutOfBoundsEx)
        assertTrue(logTry.isFailure)
      },
      test("logTry 3") {
        val logTry = LogTry(funcFatalError)
        assertTrue(logTry.isFailure)
      } @@ TestAspect.ignore,
      test("logEither with wide Type") {
        val logEither = LogEither[Exception, Int](funcArrayIndexOutOfBoundsEx)
        assertTrue(logEither.isLeft)
      },
      test("logEither with correct narrow Type") {
        val logEither = LogEither[ArithmeticException, Int](funcArithmeticEx)
        assertTrue(logEither.isLeft)
      },
      testM("logEither with incorrect narrow Type") {
        val logEither           = LogEither[ArithmeticException, Int](funcArrayIndexOutOfBoundsEx)
        val effect: UIO[String] = ZIO.fromEither(logEither).foldCause(ex => ex.toString, _ => "OK")
        assertM(effect)(equalTo("OK"))
      } @@ TestAspect.ignore
    ) @@ TestAspect.sequential
}
