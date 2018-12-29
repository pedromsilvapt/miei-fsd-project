package FSD;

public class Logger {
    public enum LogLevel {
        Debug,
        Info,
        Error,
        Fatal
    }

    private static LogLevel logLevel = LogLevel.Debug;

    public static LogLevel getLogLevel () {
        return logLevel;
    }

    public static void setLogLevel ( LogLevel logLevel ) {
        Logger.logLevel = logLevel;
    }

    public static boolean canLog ( LogLevel level ) {
        return Logger.logLevel.compareTo( level ) <= 0;
    }

    public static void log ( LogLevel level, String message ) {
        if ( Logger.canLog( level ) ) {
            System.out.printf( "[%s] %s\n", level.toString().toUpperCase(), message );
        }
    }

    public static void debug ( String message ) {
        Logger.log( LogLevel.Debug, message );
    }

    public static void debug ( String format, Object ... args ) {
        Logger.debug( String.format( format, args ) );
    }

    public static void info ( String message ) {
        Logger.log( LogLevel.Info, message );
    }

    public static void info ( String format, Object ... args ) {
        Logger.info( String.format( format, args ) );
    }

    public static void error ( String message ) {
        Logger.log( LogLevel.Error, message );
    }

    public static void error ( String format, Object ... args ) {
        Logger.error( String.format( format, args ) );
    }

    public static void error ( Exception exception ) {
        Logger.error( exception.getMessage() );
    }

    public static void fatal ( String message ) {
        Logger.log( LogLevel.Fatal, message );
    }

    public static void fatal ( String format, Object ... args ) {
        Logger.fatal( String.format( format, args ) );
    }

    public static void fatal ( Exception exception ) {
        Logger.fatal( exception.getMessage() );
    }
}
