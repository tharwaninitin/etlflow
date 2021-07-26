package etlflow.coretests.steps.steps.db

import etlflow.coretests.steps.db.DBStepTestSuite.config
import etlflow.etlsteps.DBQueryStep
import etlflow.schema.Credential.JDBC
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class DbPropsTestSuite extends AnyFlatSpec with should.Matchers {

  val step2 = DBQueryStep(
    name  = "UpdatePG",
    query = "BEGIN; DELETE FROM ratings_par WHERE 1 = 1; COMMIT;",
    credentials = JDBC(config.db.url, config.db.user, config.db.password, "org.postgresql.Driver")
  )
  val props = step2.getStepProperties()

  "getStepProperties should  " should "run successfully correct props" in {
    assert(props ==  Map("query" -> "BEGIN; DELETE FROM ratings_par WHERE 1 = 1; COMMIT;"))
  }

}
