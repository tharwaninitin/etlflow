package etlflow.webserver

import etlflow.ServerSuiteHelper
import etlflow.api.Schema.UserArgs
import zio.test.Assertion.equalTo
import zio.test._

object AuthenticationTestSuite extends DefaultRunnableSpec with ServerSuiteHelper {

  val valid_login = auth.login(UserArgs("admin","admin"))
  val invalid_login = auth.login(UserArgs("admin","admin1"))

  override def spec: ZSpec[environment.TestEnvironment, Any] =
    (suite("Authentication Test Suite")(
      testM("Authentication Test: Valid Login")(
        assertM(valid_login.map(x=> x.message))(equalTo("Valid User"))
      ),
      testM("Authentication Test: Invalid Login")(
        assertM(invalid_login.map(x=> x.message))(equalTo("Invalid User/Password"))
      )
    )@@ TestAspect.sequential).provideCustomLayer((testJsonLayer ++ testDBLayer).orDie)
}
