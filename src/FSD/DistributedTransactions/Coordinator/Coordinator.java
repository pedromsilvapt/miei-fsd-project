package FSD.DistributedTransactions.Coordinator;

import FSD.Controllable;
import FSD.DistributedTransactions.TransactionState;
import io.atomix.utils.net.Address;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface Coordinator extends Controllable< CoordinatorController > {
    Address getServer ( int index );

    int getServerIndex ( Address address );


    Collection< Transaction > getTransactions ();

    Transaction onTransactionBegin ( int[] servers );

    void onServerUpdate ( long id, int server, TransactionState state ) throws Exception;

    CompletableFuture< Void > start ();

    List< Address > getServersList ();
}