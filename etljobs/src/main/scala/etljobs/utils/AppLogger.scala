package etljobs.utils

import org.apache.log4j.jdbc.JDBCAppender
import org.apache.log4j.{BasicConfigurator, ConsoleAppender, EnhancedPatternLayout, Level, Logger}

object AppLogger {
    def initialize(): Unit = {
        // https://stackoverflow.com/questions/8965946/configuring-log4j-loggers-programmatically
        // https://github.com/apache/spark/blob/master/core/src/main/resources/org/apache/spark/log4j-defaults.properties
        val console: ConsoleAppender = new ConsoleAppender()
        console.setLayout(new EnhancedPatternLayout("%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n"))
        // console.setThreshold(Level.FATAL); // Setting LEVEL at this level will set logging for all since we are adding this as root logger
        console.setThreshold(Level.INFO)
        console.activateOptions()

        Logger.getRootLogger.addAppender(console)
        // val jdbc: JDBCAppender = new JDBCAppender()
    }  
}

//import java.sql.Connection
//import java.sql.SQLException
//import org.postgresql.ds.PGSimpleDataSource
//object ConnectionFactory {
//    private var dataSource: PGSimpleDataSource = _
//    @throws[SQLException]
//    def getConnection: Connection = {
//        if (dataSource == null) {
//            dataSource = new PGSimpleDataSource()
//            dataSource.setUrl("jdbc:postgresql://localhost:5432/etljobs")
//            dataSource.setUser("tharwanin")
//        }
//        dataSource.getConnection
//    }
//}