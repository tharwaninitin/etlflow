package etlflow.utils

import zio.test._
import zio.{UIO, ZIO}

@SuppressWarnings(Array("org.wartremover.warts.Throw"))
object ErrorHandlingTestSuite {

  def funcArithmeticEx: Int            = throw new ArithmeticException()
  def funcArrayIndexOutOfBoundsEx: Int = Array(0, 1, 2, 3, 4)(9)
  def funcFatalError: Int              = throw new StackOverflowError()

  val spec: ZSpec[environment.TestEnvironment, Any] =
    suite("Error Handling")(
      test("logTry 1") {
        val logTry = LoggedTry(funcArithmeticEx)
        assertTrue(logTry.isFailure)
      },
      test("logTry 2") {
        val logTry = LoggedTry(funcArrayIndexOutOfBoundsEx)
        assertTrue(logTry.isFailure)
      },
      test("logTry 3") {
        val logTry = LoggedTry(funcFatalError)
        assertTrue(logTry.isFailure)
      } @@ TestAspect.ignore,
      test("logEither with wide Type") {
        val logEither = LoggedEither[Exception, Int](funcArrayIndexOutOfBoundsEx)
        assertTrue(logEither.isLeft)
      },
      test("logEither with correct narrow Type") {
        val logEither = LoggedEither[ArithmeticException, Int](funcArithmeticEx)
        assertTrue(logEither.isLeft)
      },
      testM("logEither with incorrect narrow Type") {
        def logEither = LoggedEither[ArithmeticException, Int](funcArrayIndexOutOfBoundsEx)
        val effect: UIO[String] =
          ZIO.fromEither(logEither).foldCauseM(ex => ZIO.succeed(ex.squash.toString), _ => ZIO.succeed("OK"))
        assertM(effect)(Assertion.containsString("java.lang.ArrayIndexOutOfBoundsException"))
      }
    ) @@ TestAspect.sequential
}
