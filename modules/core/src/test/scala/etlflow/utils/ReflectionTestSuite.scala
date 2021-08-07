package etlflow.utils

import etlflow.EtlJobProps
import etlflow.coretests.MyEtlJobPropsMapping
import etlflow.coretests.Schema.RatingOutput
import etlflow.etljobs.EtlJob
import etlflow.utils.{ReflectAPI => RF}
import zio.{UIO, ZIO}
import zio.test.Assertion.equalTo
import zio.test._
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.{universe => ru}

object ReflectionTestSuite extends DefaultRunnableSpec {

  sealed trait EtlJobTest

  sealed trait EtlJobName

  case class Job1() extends EtlJobName

  case class Job2() extends EtlJobName

  case class Job3() extends EtlJobName with EtlJobTest

  case class Job4() extends EtlJobName with EtlJobTest

  case object Job5 extends EtlJobName

  type MEJP = MyEtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]

  def getSubClasses1[T: TypeTag]: Set[String] = {
    val tpe = ru.typeOf[T]
    val clazz = tpe.typeSymbol.asClass
    val allJobNames = clazz.knownDirectSubclasses
    allJobNames.map(x => x.name.toString)
  }

  def getSubClasses2[T: TypeTag]: List[String] = {
    val tpe = ru.typeOf[T]
    val allJobNames = tpe.baseClasses
    allJobNames.map(x => x.name.toString)
  }

  def spec: ZSpec[environment.TestEnvironment, Any] = {
    suite("Reflect Api Test Cases")(
      test("getTypeFullName[MEJP] should should return etlflow.coretests.MyEtlJobPropsMapping") {
        assert(RF.getTypeFullName[MEJP])(equalTo("etlflow.coretests.MyEtlJobPropsMapping"))
      },
      testM("getSubClasses[EtlJobName] should should retrieve Set successfully") {
        assertM(RF.getSubClasses[EtlJobName])(equalTo(Set("Job1", "Job2", "Job3", "Job4", "Job5")))
      },
      testM("getSubClasses[EtlJobTest] should  should retrieve Set successfully") {
        assertM(RF.getSubClasses[EtlJobTest])(equalTo(Set("Job3", "Job4")))
      },
      testM("getFields[RatingOutput] should  should run successfully") {
        assertM(RF.getFields[RatingOutput])(equalTo(Seq(("date_int", "Int"), ("date", "java.sql.Date"), ("timestamp", "Long"), ("rating", "Double"), ("movie_id", "Int"), ("user_id", "Int"))))
      },
      testM("getJob should return correct error message in case of invalid job name") {
        val ejpm = RF.getJob[MEJP]("JobM")
        assertM(ejpm.foldM(ex => ZIO.succeed(ex.getMessage), op => ZIO.succeed(op)))(equalTo("JobM not present"))
      },
      testM("getJob[MEJP](Job1) should return Job1") {
        val ejpm = RF.getJob[MEJP]("Job1").map(_.toString)
        assertM(ejpm)(equalTo("Job1"))
      },
      testM("getJobs[MEJP] should return list of jobs") {
        val ejpm = RF.getJobs[MEJP].map(_.map(_.name).sorted)
        assertM(ejpm)(equalTo(List("Job1", "Job2", "Job3", "Job4", "Job5", "Job6", "Job7", "Job8", "Job9")))
      },
    )
  }
}
