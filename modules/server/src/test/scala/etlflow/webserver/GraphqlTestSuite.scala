package etlflow.webserver

import caliban.Macros.gqldoc
import etlflow.ServerSuiteHelper
import etlflow.jdbc.runDbMigration
import etlflow.log.ApplicationLogger
import zio.test.Assertion.equalTo
import zio.test._

object GraphqlTestSuite extends DefaultRunnableSpec with ServerSuiteHelper with ApplicationLogger {

  val env = (testAPILayer ++ testDBLayer).orDie
  val etlFlowInterpreter = GqlAPI.api.interpreter
  val loginInterpreter = GqlLoginAPI.api.interpreter
  zio.Runtime.default.unsafeRun(runDbMigration(credentials,clean = true))

  override def spec: ZSpec[environment.TestEnvironment, Any] =
    (suite("GraphQL Test Suite")(
      testM("Test query jobs end point") {
        val query = gqldoc(
          """
           {
             jobs {
               name
               is_active
             }
           }""")
        val result = for {
          gqlResponse <- etlFlowInterpreter.flatMap(_.execute(query))
          _           = logger.info(gqlResponse.toString)
        } yield gqlResponse.data.toString
        assertM(result)(equalTo("""{"jobs":[{"name":"Job1","is_active":true},{"name":"Job2","is_active":false},{"name":"Job3","is_active":true}]}""")
        )
      },
      testM("Test query jobruns end point") {
        val query = gqldoc(
          """
             {
               jobruns(limit: 10, offset: 0) {
                job_run_id
                job_name
               }
             }""")
        assertM(etlFlowInterpreter.flatMap(_.execute(query)).map(_.data.toString))(equalTo("""{"jobruns":[{"job_run_id":"a27a7415-57b2-4b53-8f9b-5254e847a301","job_name":"EtlJobDownload"},{"job_run_id":"a27a7415-57b2-4b53-8f9b-5254e847a302","job_name":"EtlJobSpr"}]}""")
        )
      },
      testM("Test query jobruns end point with filter condition IN") {
        val query = gqldoc(
          """
             {
               jobruns(limit: 10, offset: 0, filter: "IN", jobName: "EtlJobDownload") {
                job_name
               }
             }""")
        val result = for {
          gqlResponse <- etlFlowInterpreter.flatMap(_.execute(query))
          _           = logger.info(gqlResponse.toString)
        } yield gqlResponse.data.toString
        assertM(result)(equalTo("""{"jobruns":[{"job_name":"EtlJobDownload"}]}""")
        )
      },
      testM("Test query jobruns end point with filter condition NOT IN") {
        val query = gqldoc(
          """
             {
               jobruns(limit: 10, offset: 0, filter: "NOT IN", jobName: "EtlJobDownload") {
                job_name
               }
             }""")
        val result = for {
          gqlResponse <- etlFlowInterpreter.flatMap(_.execute(query))
          _           = logger.info(gqlResponse.toString)
        } yield gqlResponse.data.toString
        assertM(result)(equalTo("""{"jobruns":[{"job_name":"EtlJobSpr"}]}""")
        )
      },
      testM("Test query stepruns end point") {
        val query = gqldoc(
          """
                 {
                   stepruns(job_run_id:"a27a7415-57b2-4b53-8f9b-5254e847a301"){
                    job_run_id
                    step_name
                   }
                 }""")
        assertM(etlFlowInterpreter.flatMap(_.execute(query)).map(_.data.toString))(equalTo("""{"stepruns":[{"job_run_id":"a27a7415-57b2-4b53-8f9b-5254e847a301","step_name":"download_spr"}]}""")
        )
      },
      testM("Test mutation login end point with correct credentials") {
        val query = gqldoc(
          """
                mutation
                {
                  login(user_name:"admin",password:"admin"){
                   message
                  }
                }""")
        assertM(loginInterpreter.flatMap(_.execute(query)).map(_.data.toString))(equalTo("""{"login":{"message":"Valid User"}}""")
        )
      },
      testM("Test mutation login end point with incorrect credentials") {
        val query = gqldoc(
          """
                mutation
                {
                  login(user_name:"admin134",password:"admin"){
                   message
                  }
                }""")
        assertM(loginInterpreter.flatMap(_.execute(query)).map(_.data.toString))(equalTo("""{"login":{"message":"Invalid User/Password"}}""")
        )
      },
      testM("Test mutation update job state end point") {
        val query = gqldoc(
          """
            mutation
                {
                  update_job_state(name: "EtlJobDownload", state: true)
                }""")

        assertM(etlFlowInterpreter.flatMap(_.execute(query)).map(_.data.toString))(equalTo("""{"update_job_state":true}""")
        )
      },
      testM("Test mutation add credentials end point") {
        val query = gqldoc(
          """
            mutation
              {
                add_credentials(name: "testing",
                type: JDBC,
                value: [ { key: "url", value: "jdbc:postgresql://localhost:5432/postgres"},
      		               { key: "user", value: "postgres"},
       		               { key: "password", value: "testing"},
                         { key: "driver", value: "org.postgresql.Driver"},
                       ]
                  ){
                 name
                }
               }""")
        val result = for {
          gqlResponse <- etlFlowInterpreter.flatMap(_.execute(query))
          _           = logger.info(gqlResponse.toString)
        } yield gqlResponse.data.toString
        assertM(result)(equalTo("""{"add_credentials":{"name":"testing"}}""".stripMargin)
        )
      },
      testM("Test mutation add credentials end point duplicate") {
        val query = gqldoc(
          """
            mutation
              {
                add_credentials(name: "testing",
                type: JDBC,
                value: [ { key: "url", value: "jdbc:postgresql://localhost:5432/postgres"},
      		               { key: "user", value: "postgres"},
       		               { key: "password", value: "testing"},
                         { key: "driver", value: "org.postgresql.Driver"},
                       ]
                  ){
                 name
                }
               }""")
        val error = """ERROR: duplicate key value violates unique constraint "credential_name_type"  Detail: Key (name, type)=(testing, jdbc) already exists."""
        assertM(etlFlowInterpreter.flatMap(_.execute(query)).map(_.errors.map(_.getMessage.filter(_ >= ' '))))(equalTo(List(error))
        )
      },
      testM("Test mutation add credentials end point with incorrect input") {
        val query = gqldoc(
          """
            mutation
              {
                add_credentials(name: "testing",
                type: testing,
                value: [ { key: "url", value: "jdbc:postgresql://localhost:5432/postgres"},
      		               { key: "user", value: "postgres"},
       		               { key: "password", value: "testing"},
                         { key: "driver", value: "org.postgresql.Driver"},
                       ]
                  ){
                 name
                }
               }""")
        assertM(etlFlowInterpreter.flatMap(_.execute(query)).map(_.errors.map(_.getMessage)))(equalTo(List("""Invalid value testing for trait Creds"""))
        )
      },
      testM("Test mutation update credentials end point") {
        val query = gqldoc(
          """
            mutation
              {
                update_credentials(name: "testing",
                type: JDBC,
                value: [ { key: "url", value: "jdbc:postgresql://localhost:5432/postgres"},
      		               { key: "user", value: "postgres123"},
       		               { key: "password", value: "testing"},
                         { key: "driver", value: "org.postgresql.Driver"},
                       ]
                  ){
                 name
                }
               }""")

        assertM(etlFlowInterpreter.flatMap(_.execute(query)).map(_.data.toString))(equalTo("""{"update_credentials":{"name":"testing"}}""".stripMargin)
        )
      },
      testM("Test query get credentials end point") {
        val query = gqldoc(
          """
              query{
                credential{
                  name
                  type
                }
              }""")
        assertM(etlFlowInterpreter.flatMap(_.execute(query)).map(_.data.toString))(equalTo("""{"credential":[{"name":"testing","type":"jdbc"}]}""".stripMargin)
        )
      }
    ) @@ TestAspect.sequential).provideCustomLayerShared(env)
}
