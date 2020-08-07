package etlflow.scheduler
import cats.effect.Blocker
import doobie.util.ExecutionContexts
import doobie.util.transactor.Transactor
import doobie.util.transactor.Transactor.Aux
import etlflow.utils.GlobalProperties
import zio.Task
import zio.interop.catz._

import scala.util.Try

trait TestSuiteHelper  {

  val cache = CacheHelper.createCache[String](60)
  val canonical_path: String               = new java.io.File(".").getCanonicalPath
  val global_properties: Option[GlobalProperties] =
    Try(new GlobalProperties(canonical_path + "/modules/scheduler/src/test/resources/loaddata.properties") {}).toOption

  val transactor: Aux[Task, Unit] = Transactor.fromDriverManager[Task](
    global_properties.get.log_db_driver,     // driver classname
    global_properties.get.log_db_url,     // connect URL (driver-specific)
    global_properties.get.log_db_user,                  // user
    global_properties.get.log_db_pwd,                          // password
      Blocker.liftExecutionContext(ExecutionContexts.synchronous) // just for testing
    )

}
