package etlflow

import cats.implicits._
import java.time.LocalDateTime
import cats.effect.Resource
import skunk._
import skunk.implicits._
import skunk.codec.all._
import zio.Task
import natchez.Trace.Implicits.noop
import zio.interop.catz._

trait PGSkunkUtils {

  case class QueryMetrics(start_time:LocalDateTime, email:String, query:String, duration:Double, status:String) {
    override def toString: String = s"$start_time $email $duration $status"
  }

  object QueryMetrics {
    val codec: Codec[QueryMetrics] = (timestamp, varchar, text, float8, varchar).imapN(QueryMetrics.apply)(QueryMetrics.unapply(_).get)
  }

  val session: Resource[Task, Session[Task]] = Session.single(
      host = "localhost",
      port = 5432,
      user = System.getProperty("user.name"),
      database = "postgres",
    )

  def insertDb(record: QueryMetrics): Task[Unit] = {

    val insert: Command[QueryMetrics] = sql"INSERT INTO BQDUMP VALUES (${QueryMetrics.codec})".command

    session.use{ s =>
      s.prepare(insert).use{ pc =>
        pc.execute(record)
      }
    }.as(())

    //            val stream: Stream[Task, Unit] = for {
    //              s  <- Stream.resource(session)
    //              pq <- Stream.resource(s.prepare(insert))
    //              c  <- Stream.eval(pq.execute(record))
    //            } yield ()
    //
    //            stream.compile.drain
  }

}
