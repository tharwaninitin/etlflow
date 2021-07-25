package etlflow.coretests.steps.credential

import etlflow.etlsteps.GetCredentialStep
import etlflow.schema.Credential.JDBC
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import etlflow.utils.CredentialImplicits._

class CredentialPropsTestSuite extends AnyFlatSpec with should.Matchers {

  val step2 =  GetCredentialStep[JDBC](
    name  = "GetCredential",
    credential_name = "etlflow",
  )

  val props = step2.getStepProperties()
  "GetTimeDifferenceAsString should  " should "run successfully for days" in {
    assert(props ==  Map("credential_name" -> "etlflow"))
  }

}
