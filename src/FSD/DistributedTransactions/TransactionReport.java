package DistributedTransactions;

public class TransactionReport {
    public long             id;
    public TransactionState state;

    public TransactionReport () {}

    public TransactionReport ( long id, TransactionState state ) {
        this.id = id;
        this.state = state;
    }

    public String toString () {
        return String.format( "TransactionReport( id = %d, state = %s )", this.id, this.state.toString() );
    }
}
