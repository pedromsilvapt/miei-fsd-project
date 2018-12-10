package DistributedTransactions.Participant;

import DistributedTransactions.TransactionState;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

public interface Participant < T > {
    // Tries to write all the given data blocks to the log file
    // After that, waits for a decision from the coordinator about the transaction state: Commit or Abort
    // Returns true if the transaction is committed
    CompletableFuture< Boolean > tryCommit ( long transaction, Collection< T > blocks );

    void onCoordinatorUpdate ( long transaction, TransactionState state );

    void release ( long transaction );

    CompletableFuture< Void > start ();
}
