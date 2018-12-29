package FSD.DistributedTransactions.Participant;

import FSD.DistributedTransactions.TransactionState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class Transaction<T> {
    public long id;

    public TransactionState state;

    public CompletableFuture< Boolean > future;

    public List<T> blocks = new ArrayList<>();

    public Transaction () { }

    public Transaction ( long id, TransactionState state ) {
        this.id = id;
        this.state = state;
        this.future = new CompletableFuture<>();
    }

    public Transaction ( long id, TransactionState state, Collection<T> blocks ) {
        this.id = id;
        this.state = state;
        this.future = new CompletableFuture<>();
        this.blocks = new ArrayList<>( blocks );
    }

    public String toString () {
        return String.format( "Transaction@%s( id = %d, state = %s )", this.hashCode(), this.id, this.state );
    }
}
