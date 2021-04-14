package etlflow.jdbc

import cats.effect.{ContextShift, IO}
import doobie.util.{ExecutionContexts, log}
import doobie.util.transactor.Transactor
import etlflow.ServerSuiteHelper
import etlflow.utils.EtlFlowHelper.{CredentialDB, JsonString}
import org.scalatest._

class DoobieTestSuite extends funsuite.AnyFunSuite with matchers.should.Matchers with doobie.scalatest.IOChecker with ServerSuiteHelper {
  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContexts.synchronous)
  implicit val dbLogger: log.LogHandler = DBLogger()
  val transactor: doobie.Transactor[IO] = Transactor.fromDriverManager[IO](credentials.driver, credentials.url, credentials.user, "")
  val creds: CredentialDB = CredentialDB("test", "jdbc", JsonString("{}"))
  val query: doobie.Update0 = SQL.addCredentials(creds)
  test("addCredentials") { check(query) }
}
