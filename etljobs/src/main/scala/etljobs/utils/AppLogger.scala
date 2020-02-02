package etljobs.utils

import org.apache.log4j.{Level, Logger, BasicConfigurator, ConsoleAppender, PatternLayout}

object AppLogger {
    def initialize(): Unit = {
        // https://stackoverflow.com/questions/8965946/configuring-log4j-loggers-programmatically
        // https://github.com/apache/spark/blob/master/core/src/main/resources/org/apache/spark/log4j-defaults.properties
        val console: ConsoleAppender = new ConsoleAppender(); 
        console.setLayout(new PatternLayout("%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n")); 
        // console.setThreshold(Level.FATAL); // Setting LEVEL at this level will set logging for all since we are adding this as root logger
        console.activateOptions();
        Logger.getRootLogger().addAppender(console)
    }  
}