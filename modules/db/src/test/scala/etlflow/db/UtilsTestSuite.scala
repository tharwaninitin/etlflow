package etlflow.db

import etlflow.db.Sql.getStartTime
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.text.SimpleDateFormat
import java.time.LocalDate

class UtilsTestSuite extends AnyFlatSpec with should.Matchers {

  val sdf = new SimpleDateFormat("yyyy-MM-dd")
  "Provided Exception" should "return" in {
    assert(getStartTime(None) == sdf.parse(LocalDate.now().toString).getTime)
  }
}
