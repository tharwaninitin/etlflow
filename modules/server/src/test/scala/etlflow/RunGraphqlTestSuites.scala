package etlflow

import etlflow.db.RunDbMigration
import etlflow.webserver.GraphqlTestSuite
import zio.test._

object RunGraphqlTestSuites extends DefaultRunnableSpec with ServerSuiteHelper {
  zio.Runtime.default.unsafeRun(RunDbMigration(credentials,clean = true))

  def spec: ZSpec[environment.TestEnvironment, Any] = suite("Graphql Test Suites") (
    GraphqlTestSuite.spec,
  ).provideCustomLayerShared(fullLayer.orDie)
}
