package FSD.Client;

import FSD.DistributedMap.MapNodeController.PutRequest;
import FSD.DistributedTransactions.Participant.LogEntry;
import FSD.DistributedTransactions.Participant.LogEntryType;
import FSD.DistributedTransactions.TransactionReport;
import FSD.DistributedTransactions.TransactionRequest;
import FSD.DistributedTransactions.TransactionState;
import FSD.Logger;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

public class ClientController {
    private Client client;
    private Address coordinator;
    private Serializer serializer;
    private ExecutorService executorService;
    private ManagedMessagingService channel;

    public ClientController(Client client, Address address, Address coordinator) {
        this.coordinator = coordinator;

        this.client = client;

        this.client.setController( this );

        this.serializer = Serializer.builder()
                .withTypes( PutRequest.class )
                .withTypes( ArrayList.class )
                .withTypes( TransactionRequest.class )
                .withTypes( TransactionReport.class )
                .withTypes( TransactionState.class )
                .withTypes( TransactionReport.class )
                .withTypes( TransactionState.class )
                .withTypes( LogEntryType.class )
                .withTypes( LogEntry.class )
                .build();

        this.channel = NettyMessagingService
                .builder()
                .withAddress( address )
                .build();

        this.executorService = Executors.newSingleThreadExecutor();
    }

    public CompletableFuture<Boolean> putRequest(long transaction, Address server, Map<Long, byte[]> data) {
        PutRequest request = new PutRequest(transaction, data);
        return channel.sendAndReceive(server, "put", serializer.encode(request))
                      .thenApply(this.serializer::decode);
    }

    public CompletableFuture<Map<Long, byte[]>> getRequest(Address server, Collection<Long> values) {
        return channel.sendAndReceive( server, "get", serializer.encode(values) ).thenApply( serializer::decode );
    }

    public CompletableFuture< Long > createTransaction ( List<Integer> participants ) {
        TransactionRequest request = new TransactionRequest( participants );

        return channel.sendAndReceive(coordinator, "create-transaction", serializer.encode(request)).thenApply( serializer::decode );
    }

    public CompletableFuture<List<String>> discoverParticipants() {
        return channel.sendAndReceive(coordinator, "discover-participants", new byte[0] ).thenApply( serializer::decode );
    }

    public CompletableFuture< Void > start () {
        return this.channel.start()
                .thenCompose( m -> this.client.start() )
                .thenApply( a -> null);
    }
}
