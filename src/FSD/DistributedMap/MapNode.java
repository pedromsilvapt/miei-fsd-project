package FSD.DistributedMap;

import FSD.Controllable;
import FSD.DistributedTransactions.Participant.Participant;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface MapNode extends Controllable< MapNodeController > {
    Participant< ChangeLog< Long, byte[] > > getTransactions ();

    CompletableFuture< Boolean > put ( long transaction, Map< Long, byte[] > data );

    CompletableFuture< Map< Long, byte[] > > get ( Collection< Long > keys );

    CompletableFuture<Void> start ();
}
