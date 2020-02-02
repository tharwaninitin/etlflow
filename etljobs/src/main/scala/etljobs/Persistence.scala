// package etljobs

// import io.getquill._
// import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

// object Persistence {
//   val pgDataSource = new org.postgresql.ds.PGSimpleDataSource()
//   pgDataSource.setDatabaseName("<-->")
//   pgDataSource.setUser("<-->")
//   val config = new HikariConfig()
//   config.setDataSource(pgDataSource)
//   val ctx = new PostgresJdbcContext(LowerCase, new HikariDataSource(config))

//   import ctx._

//   case class Job(
//                   id: String,
//                   description: Option[String],
//                   properties: Option[String],
//                   start_date: Option[java.util.Date],
//                   end_date: Option[java.util.Date],
//                   notification: Option[String]
//                 )

//   val q = quote {
//     querySchema[Job]("job", _.notification -> "notify")
//   }

// //  ctx.run(q)
// }
