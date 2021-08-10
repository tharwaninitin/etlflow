package etlflow.db

import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object ScalaLikeSqlTestSuite extends DefaultRunnableSpec {
    override def spec: ZSpec[environment.TestEnvironment, Any] =
    (suite("SQL Suite")(
        test("insertJobs Sql")({
            val seq = Seq(
                Seq("Job1", "", "", 0, 0, true),
                Seq("Job2", "", "", 0, 0, true),
            )
            val ip = ScalaLikeSQL.insertJobs(seq).statement
            val op = """insert into Job values (?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?) ON CONFLICT(job_name) DO UPDATE SET schedule = EXCLUDED.schedule"""
            assert(ip)(equalTo(op))
        }),
        test("insertJobs Params")({
            val seq = Seq(
                Seq("Job1", "", "", 0, 0, true),
                Seq("Job2", "", "", 0, 0, true),
            )
            val ip = ScalaLikeSQL.insertJobs(seq).parameters
            val op = Seq("Job1", "", "", 0, 0, true,"Job2", "", "", 0, 0, true)
            assert(ip)(equalTo(op))
        })
    ))
}
