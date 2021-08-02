package etlflow.coretests.utils

import etlflow.coretests.Schema.RatingOutput
import etlflow.coretests.TestSuiteHelper
import etlflow.utils.{ReflectAPI => RF}
import zio.test.Assertion.equalTo
import zio.test.{DefaultRunnableSpec, ZSpec, assertM, environment, _}
import zio.{Task, ZIO}

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.{universe => ru}

object ReflectionTestSuite extends DefaultRunnableSpec with TestSuiteHelper {

  sealed trait EtlJob
  sealed trait EtlJobName

  case class Job1() extends EtlJobName
  case class Job2() extends EtlJobName
  case class Job3() extends EtlJobName with EtlJob
  case class Job4() extends EtlJobName with EtlJob
  case object Job5 extends EtlJobName

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
      test("getEtlJobs(EtlJobName) should should retrieve Set successfully") {
        assert(RF.getEtlJobs[EtlJobName])(equalTo(Set("Job1", "Job2", "Job3", "Job4", "Job5")))
      },
      test("getEtlJobs(EtlJob) should  should retrieve Set successfully") {
         assert(RF.getEtlJobs[EtlJob])(equalTo(Set("Job3", "Job4")))
      },
      test("getFields[RatingOutput] should  should run successfully") {
        assert(RF.getFields[RatingOutput])(equalTo(Seq(("date_int","Int"), ("date","java.sql.Date"), ("timestamp","Long"), ("rating","Double"), ("movie_id","Int"), ("user_id","Int"))))
      },
      testM("GetEtlJobPropsMapping should return correct  error message") {
        val x = Task(RF.getEtlJobPropsMapping[MEJP]("Job1","")).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("ok"))
        assertM(x)(equalTo("Job1 not present"))
      }
    )
  }
}
