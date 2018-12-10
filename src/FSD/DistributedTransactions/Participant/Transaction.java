package FSD.DistributedTransactions.Participant;

import FSD.DistributedTransactions.TransactionState;

import java.util.concurrent.CompletableFuture;

public class Transaction {
    public long id;

    public TransactionState state;

    public CompletableFuture< Boolean > future;

    public boolean aborted;

    public Transaction () { }

    public Transaction ( long id, TransactionState state ) {
        this.id = id;
        this.state = state;
        this.future = new CompletableFuture<>();
    }
}
