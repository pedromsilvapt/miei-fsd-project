package DistributedTransactions.Participant;

public class LogEntry < T > {
    public long         id;
    public T            data;
    public LogEntryType type;

    public LogEntry () { }

    public LogEntry ( long id, LogEntryType type ) {
        this( id, type, null );
    }

    public LogEntry ( long id, LogEntryType type, T data ) {
        this.id = id;
        this.data = data;
        this.type = type;
    }
}
