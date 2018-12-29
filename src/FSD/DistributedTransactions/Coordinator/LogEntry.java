package FSD.DistributedTransactions.Coordinator;

import FSD.DistributedTransactions.TransactionState;

import java.util.Arrays;

public class LogEntry {
    public long             tid;
    public TransactionState type;
    public int[]            servers;

    public LogEntry () {}

    public LogEntry ( long id, TransactionState state ) {
        this.tid = id;
        this.type = state;
    }

    public LogEntry ( long id, int[] servers, TransactionState type ) {
        this.tid = id;
        this.type = type;
        this.servers = servers;
    }

    @Override
    public String toString () {
        return String.format( "LogEntry( tid = %d, servers = %s, state = %s )", this.tid, Arrays.toString( this.servers ), this.type );
    }
}