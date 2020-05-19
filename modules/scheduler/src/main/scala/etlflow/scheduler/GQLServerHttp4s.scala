package etlflow.scheduler

import caliban.{CalibanError, GraphQLInterpreter, Http4sAdapter}
import doobie.hikari.HikariTransactor
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl
import org.http4s.server.Router
import org.http4s.server.blaze.BlazeServerBuilder
import zio.console.putStrLn
import zio.{RIO, ZIO, _}
import zio.interop.catz._
import org.http4s.implicits._
import scala.concurrent.duration._
import etlflow.{BuildInfo => BI}

private[scheduler] trait GQLServerHttp4s extends CatsApp {
  val DB_DRIVER: String   // driver classname
  val DB_URL: String      // connect URL
  val DB_USER: String     // username
  val DB_PASS: String     // password

  def etlFlowHttp4sInterpreter(transactor: HikariTransactor[Task]): ZIO[Any, CalibanError, GraphQLInterpreter[ZEnv, Throwable]]

  type EtlFlowTask[A] = RIO[ZEnv, A]

  object ioz extends Http4sDsl[EtlFlowTask]
  import ioz._
  val otherRoutes:HttpRoutes[EtlFlowTask] = HttpRoutes.of[EtlFlowTask] {
    case _@GET -> Root => Ok(s"Hello, Welcome to EtlFlow API ${BI.version}, Build with scala version ${BI.scalaVersion}")
  }

  override def run(args: List[String]): ZIO[ZEnv, Nothing, Int] = {
    val serverManaged = for {
      dbTransactor         <- DbHelpers.dbResource(DB_DRIVER,DB_URL,DB_USER,DB_PASS).toManaged
      etlFlowInterpreter   <- etlFlowHttp4sInterpreter(dbTransactor).toManaged_
      server               <- BlazeServerBuilder[EtlFlowTask]
                                 .bindHttp(8080, "0.0.0.0")
                                 .withConnectorPoolSize(2)
                                 .withResponseHeaderTimeout(60.seconds)
                                 .withIdleTimeout(60.seconds)
                                 .withExecutionContext(platform.executor.asEC)
                                 .withHttpApp(
                                   Router[EtlFlowTask](
                                     "/"    -> otherRoutes,
                                     "/api/etlflow"    -> Http4sAdapter.makeHttpService(etlFlowInterpreter),
                                     "/ws/etlflow"     -> Http4sAdapter.makeWebSocketService(etlFlowInterpreter),
                                   ).orNotFound
                                 )
                                 .resource
                                 .toManaged

   } yield server
  serverManaged.useForever.as(0).catchAll(err => putStrLn(err.toString).as(1))
  }
}
