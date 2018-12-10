package FSD.DistributedTransactions.Coordinator;

import FSD.DistributedTransactions.TransactionState;
import io.atomix.utils.net.Address;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public interface Coordinator {
    Address getServer ( int index );

    int getServerIndex ( Address address );


    Collection< Transaction > getTransactions ();

    Transaction onTransactionBegin ( int[] servers );

    void onServerUpdate ( long id, int server, TransactionState state ) throws Exception;

    void setTransactionListener ( Consumer<Transaction> listener );

    void onTransactionPrepared ( long id );

    void onTransactionFulfilled ( long id, TransactionState state );

    CompletableFuture< Void > start ();

    List<Address> getServersList ();
}