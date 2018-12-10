package DistributedTransactions.Coordinator;

import DistributedTransactions.TransactionState;
import io.atomix.utils.net.Address;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

public interface Coordinator {
    Address getServer ( int index );

    int getServerIndex ( Address address );


    Collection< Transaction > getTransactions ();

    Transaction onTransactionBegin ( int[] servers );

    void onServerUpdate ( long id, int server, TransactionState state ) throws Exception;

    void onTransactionPrepared ( long id );

    void onTransactionFulfilled ( long id, TransactionState state );

    CompletableFuture< Void > start ();
}