package etlflow.steps.gcp

import etlflow.coretests.Schema.RatingOutput
import etlflow.gcp.{ReflectAPI => RF}
import zio.test.Assertion.equalTo
import zio.test._

object ReflectionTestSuite extends DefaultRunnableSpec {

  def spec: ZSpec[environment.TestEnvironment, Any] = {
    suite("Reflect Api Test Cases")(
      test("getFields[RatingOutput] should run successfully") {
        assert(RF.getFields[RatingOutput])(equalTo(Seq(("date_int", "Int"), ("date", "java.sql.Date"), ("timestamp", "Long"), ("rating", "Double"), ("movie_id", "Int"), ("user_id", "Int"))))
      }
    ) @@ TestAspect.sequential
  }
}
