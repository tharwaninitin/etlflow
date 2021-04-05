package etlflow.webserver.api

import caliban.Macros.gqldoc
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import zio.test.Assertion.equalTo
import zio.test._

object GraphqlTestSuite extends DefaultRunnableSpec with TestEtlFlowService {

  val env = Clock.live ++ Blocking.live ++ testHttp4s(transactor,cache) ++ Console.live
  val etlFlowInterpreter = EtlFlowApi.api.interpreter
  val loginInterpreter   = LoginApi.api.interpreter
  zio.Runtime.default.unsafeRun(runDbMigration(credentials,clean = true))

  override def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("GraphQL Test Suite")(
//      testM("Test jobs end point") {
//        val query = gqldoc(
//          """
//           {
//             queueStats {
//                job_name
//                submitted_from
//             }
//           }""")
//        assertM(etlFlowInterpreter.flatMap(_.execute(query)).provideLayer(env).map(_.data.toString))(equalTo("""{"jobs":[{"name":"EtlJobDownload","job_deploy_mode":"LOCAL","max_active_runs":10,"is_active":true}]}""")
//        )
//      },
      testM("Test jobs end point") {
        val query = gqldoc(
          """
           {
             jobs {
               name
               job_deploy_mode
               max_active_runs
               is_active
             }
           }""")
        assertM(etlFlowInterpreter.flatMap(_.execute(query)).provideLayer(env).map(_.data.toString))(equalTo("""{"jobs":[{"name":"EtlJobDownload","job_deploy_mode":"LOCAL","max_active_runs":10,"is_active":true},{"name":"Job1","job_deploy_mode":"LOCAL","max_active_runs":10,"is_active":true}]}""")
        )
      },
      testM("Test jobruns end point") {
        val query = gqldoc(
          """
             {
               jobruns(limit: 10, offset: 0) {
                job_run_id
                job_name
               }
             }""")
        assertM(etlFlowInterpreter.flatMap(_.execute(query)).provideLayer(env).map(_.data.toString))(equalTo("""{"jobruns":[{"job_run_id":"a27a7415-57b2-4b53-8f9b-5254e847a301","job_name":"EtlJobDownload"},{"job_run_id":"a27a7415-57b2-4b53-8f9b-5254e847a302","job_name":"EtlJobSpr"}]}""")
        )
      },
      testM("Test jobruns end point with filter condition IN") {
        val query = gqldoc(
          """
             {
               jobruns(limit: 10, offset: 0, filter: "IN", jobName: "EtlJobDownload") {
                job_name
               }
             }""")
        val result = for {
          gqlResponse <- etlFlowInterpreter.flatMap(_.execute(query)).provideLayer(env)
          _           = logger.info(gqlResponse.toString)
        } yield gqlResponse.data.toString
        assertM(result)(equalTo("""{"jobruns":[{"job_name":"EtlJobDownload"}]}""")
        )
      },
      testM("Test jobruns end point with filter condition NOT IN") {
        val query = gqldoc(
          """
             {
               jobruns(limit: 10, offset: 0, filter: "NOT IN", jobName: "EtlJobDownload") {
                job_name
               }
             }""")
        val result = for {
          gqlResponse <- etlFlowInterpreter.flatMap(_.execute(query)).provideLayer(env)
          _           = logger.info(gqlResponse.toString)
        } yield gqlResponse.data.toString
        assertM(result)(equalTo("""{"jobruns":[{"job_name":"EtlJobSpr"}]}""")
        )
      },
      testM("Test stepruns end point") {
        val query = gqldoc(
          """
                 {
                   stepruns(job_run_id:"a27a7415-57b2-4b53-8f9b-5254e847a301"){
                    job_run_id
                    step_name
                   }
                 }""")
        assertM(etlFlowInterpreter.flatMap(_.execute(query)).provideLayer(env).map(_.data.toString))(equalTo("""{"stepruns":[{"job_run_id":"a27a7415-57b2-4b53-8f9b-5254e847a301","step_name":"download_spr"}]}""")
        )
      },
      testM("Test Login end point with postive case") {
        val query = gqldoc(
          """
                mutation
                {
                  login(user_name:"admin",password:"admin"){
                   message
                  }
                }""")
        assertM(loginInterpreter.flatMap(_.execute(query)).provideLayer(env).map(_.data.toString))(equalTo("""{"login":{"message":"Valid User"}}""")
        )
      },
      testM("Test Login end point with negative case") {
        val query = gqldoc(
          """
                mutation
                {
                  login(user_name:"admin134",password:"admin"){
                   message
                  }
                }""")
        assertM(loginInterpreter.flatMap(_.execute(query)).provideLayer(env).map(_.data.toString))(equalTo("""{"login":{"message":"Invalid User"}}""")
        )
      },
      testM("Test update job state end point") {
        val query = gqldoc(
          """
            mutation
                {
                  update_job_state(name: "EtlJobDownload", state: true)
                }""")

        assertM(etlFlowInterpreter.flatMap(_.execute(query)).provideLayer(env).map(_.data.toString))(equalTo("""{"update_job_state":true}""")
        )
      },
//      testM("Test add cron job end point") {
//        val query = gqldoc(
//          """
//            mutation
//                {
//                  add_cron_job(job_name: "EtlJobDownload123", schedule: "0 15 * * * ?"){
//                   job_name
//                  }
//                }""")
//
//        assertM(etlFlowInterpreter.flatMap(_.execute(query)).provideLayer(env).map(_.data.toString))(equalTo("""{"add_cron_job":{"job_name":"EtlJobDownload123"}}""")
//        )
//      },
//      testM("Test update cron job end point") {
//        val query = gqldoc(
//          """
//            mutation
//                {
//                  update_cron_job(job_name: "EtlJobDownload123", schedule: "0 15 * * * ?"){
//                   job_name
//                  }
//                }""")
//
//        assertM(etlFlowInterpreter.flatMap(_.execute(query)).provideLayer(env).map(_.data.toString))(equalTo("""{"update_cron_job":{"job_name":"EtlJobDownload123"}}""")
//        )
//      },
      testM("Test add credentials end point") {
        val query = gqldoc(
          """
            mutation
              {
                add_credentials(name: "flyway_testing",
                type: JDBC,
                value: [ { key: "url", value: "jdbc:postgresql://localhost:5432/postgres"},
      		               { key: "user", value: "postgres"},
       		               { key: "password", value: "swap123"},
                         { key: "driver", value: "org.postgresql.Driver"},
                       ]
                  ){
                 name
                }
               }""")

        assertM(etlFlowInterpreter.flatMap(_.execute(query)).provideLayer(env).map(_.data.toString))(equalTo("""{"add_credentials":{"name":"flyway_testing"}}""".stripMargin)
        )
      },
      testM("Test add credentials end point negative scenario") {
        val query = gqldoc(
          """
            mutation
              {
                add_credentials(name: "flyway_testing",
                type: 123,
                value: [ { key: "url", value: "jdbc:postgresql://localhost:5432/postgres"},
      		               { key: "user", value: "postgres"},
       		               { key: "password", value: "swap123"},
                         { key: "driver", value: "org.postgresql.Driver"},
                       ]
                  ){
                 name
                }
               }""")
        assertM(etlFlowInterpreter.flatMap(_.execute(query)).provideLayer(env).map(_.errors.map(_.getMessage)))(equalTo(List("""Can't build a String from input [{"key":"url","value":"jdbc:postgresql://localhost:5432/postgres"},{"key":"user","value":"postgres"},{"key":"password","value":"swap123"},{"key":"driver","value":"org.postgresql.Driver"}]"""))
        )
      },
      testM("Test update credentials end point") {
        val query = gqldoc(
          """
            mutation
              {
                update_credentials(name: "flyway_testing",
                type: JDBC,
                value: [ { key: "url", value: "jdbc:postgresql://localhost:5432/postgres"},
      		               { key: "user", value: "postgres123"},
       		               { key: "password", value: "swap123"},
                         { key: "driver", value: "org.postgresql.Driver"},
                       ]
                  ){
                 name
                }
               }""")

        assertM(etlFlowInterpreter.flatMap(_.execute(query)).provideLayer(env).map(_.data.toString))(equalTo("""{"update_credentials":{"name":"flyway_testing"}}""".stripMargin)
        )
      }
    )
}
