package etlflow.webserver

import etlflow.ServerSuiteHelper
import etlflow.api.{EtlFlowTask, ServerEnv}
import org.http4s._
import org.http4s.client.Client
import org.http4s.dsl.Http4sDsl
import org.http4s.implicits._
import zio.interop.catz._
import zio.test.Assertion._
import zio.test._
import zio.{Task, ZIO}

object RestTestSuite extends DefaultRunnableSpec with ServerSuiteHelper with Http4sDsl[EtlFlowTask] {

  zio.Runtime.default.unsafeRun(runDbMigration(credentials,clean = true))
  val env = (testAPILayer ++ testDBLayer).orDie

  private def apiResponse(apiRequest: Request[EtlFlowTask]): ZIO[ServerEnv, Throwable, Either[String, String]] =
    (for {
      client <- Task(Client.fromHttpApp[EtlFlowTask](RestAPINew.routes.orNotFound))
      output <- client.run(apiRequest).use{
        case Status.Successful(r) => r.attemptAs[String].leftMap(_.message).value
        case r => r.as[String].map(output => Left(s"${r.status.code}, $output"))
      }
    } yield output).fold(ex => Left(s"500, ${ex.getMessage}"), op => op)

  override def spec: ZSpec[environment.TestEnvironment, Any] =
    (suite("Rest Test Suite")(
      testM("Test REST runjob end point with correct job name") {
        val apiRequest: Request[EtlFlowTask] = Request[EtlFlowTask](method = POST, uri = uri"/runjob/Job1")
        val output = """{"name":"Job1","props":{"job_send_slack_notification":"false","job_enable_db_logging":"true","job_notification_level":"info","job_max_active_runs":"10","job_name":"etlflow.coretests.jobs.Job1HelloWorld","job_description":"","job_props_name":"etlflow.coretests.Schema$EtlJob1Props","job_deploy_mode":"local","job_retry_delay_in_minutes":"0","job_status":"ACTIVE","job_schedule":"0 */2 * * * ?","job_retries":"0"}}"""
        assertM(apiResponse(apiRequest))(equalTo(Right(output)))
      },
      testM("Test REST runjob end point with incorrect job name") {
        val apiRequest: Request[EtlFlowTask] = Request[EtlFlowTask](method = POST, uri = uri"/runjob/InvalidJob")
        assertM(apiResponse(apiRequest))(equalTo(Left("400, InvalidJob not present")))
      }
    ) @@ TestAspect.sequential).provideCustomLayerShared(env)
}
