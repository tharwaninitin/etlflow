package etlflow.db

import zio.test.Assertion.equalTo
import zio.test._
import java.text.SimpleDateFormat
import java.time.LocalDate

object UtilsTestSuite{
  val sdf = new SimpleDateFormat("yyyy-MM-dd")
  val spec: ZSpec[environment.TestEnvironment, Any] = {
    suite("UtilsTestSuite Api")(
      test("Provided Exception should return") {
        assert(Utils.getStartTime(None))(equalTo(sdf.parse(LocalDate.now().toString).getTime))
      }
    )
  }
}
