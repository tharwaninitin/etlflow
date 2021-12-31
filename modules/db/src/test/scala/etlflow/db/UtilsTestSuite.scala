package etlflow.db

import zio.test._
import java.text.SimpleDateFormat
import java.time.LocalDate

object UtilsTestSuite {
  val sdf = new SimpleDateFormat("yyyy-MM-dd")
  val spec: ZSpec[environment.TestEnvironment, Any] = {
    suite("UtilsTestSuite Api")(
      test("Provided Exception should return") {
        assertTrue(Utils.getStartTime(None) == sdf.parse(LocalDate.now().toString).getTime)
      }
    )
  }
}
