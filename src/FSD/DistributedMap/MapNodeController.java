package FSD.DistributedMap;

import FSD.DistributedTransactions.Participant.ParticipantController;
import FSD.DistributedTransactions.TransactionReport;
import FSD.DistributedTransactions.TransactionState;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MapNodeController {
    public static class PutRequest {
        long transaction;
        Map<Long, byte[]> data;

        public PutRequest () { }

        public PutRequest ( long transaction, Map<Long, byte[]> data ) {
            this.transaction = transaction;
            this.data = data;
        }
    }

    private MapNode                                            node;
    private ParticipantController< ChangeLog< Long, byte[] > > participantController;
    private Serializer                                         serializer;
    private ManagedMessagingService                            messagingService;
    private ExecutorService                                    executorService;
    private CompletableFuture< Void >                          starter;

    public MapNodeController ( MapNode node, Address address, Address coordinator ) {
        this.node = node;

        this.node.setController( this );

        Class< ? >[] types = node.getTransactions().getSerializableTypes();

        this.serializer = Serializer.builder()
                .withTypes( types )
                .withTypes( Boolean.class )
                .withTypes( PutRequest.class )
                .withTypes( TransactionReport.class )
                .withTypes( TransactionState.class )
                .build();

        this.messagingService = NettyMessagingService
                .builder()
                .withAddress( address )
                .build();

        this.executorService = Executors.newSingleThreadExecutor();

        this.participantController = new ParticipantController<>( this.node.getTransactions(), this.serializer, this.messagingService, this.executorService, coordinator );

        this.messagingService.registerHandler( "get", this::onGetRequest );
        this.messagingService.registerHandler( "put", this::onPutRequest );
    }

    public CompletableFuture< byte[] > onGetRequest ( Address origin, byte[] message ) {
        Collection<Long> keys = this.serializer.decode( message );

        return this.node.get( keys ).thenApply( this.serializer::encode );
    }

    public CompletableFuture< byte[] > onPutRequest ( Address origin, byte[] message ) {
        PutRequest request = this.serializer.decode( message );

        return this.node.put( request.transaction, request.data ).thenApply( this.serializer::encode );
    }

    public CompletableFuture< Void > start () {
        // Prevent multiple start calls
        if ( this.starter != null ) {
            return this.starter;
        }

        return this.starter = this.messagingService.start().thenCompose( a -> this.node.start() );
    }
}
