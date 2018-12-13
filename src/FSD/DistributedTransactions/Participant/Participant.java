package FSD.DistributedTransactions.Participant;

import FSD.Controllable;
import FSD.DistributedTransactions.TransactionState;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

public interface Participant < T > extends Controllable< ParticipantController< T > > {
    Class< ? >[] getSerializableTypes ();

    String getName ();

    void onCoordinatorUpdate ( long transaction, TransactionState state );

    // Tries to write all the given data blocks to the log file
    // After that, waits for a decision from the coordinator about the transaction state: Commit or Abort
    // Returns true if the transaction is committed
    CompletableFuture< Boolean > tryCommit ( long transaction, Collection< T > blocks );

    // Allows external conditions to abort a transaction. Only works if the transaction is at most
    // in the waiting stage
    CompletableFuture< Void > abort ( long transaction );

    // Released transactions can be eliminated from the log
    void release ( long transaction );

    CompletableFuture< Void > start ();
}
