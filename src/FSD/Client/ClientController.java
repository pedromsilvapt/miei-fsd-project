package FSD.Client;

import FSD.DistributedMap.MapNodeController.PutRequest;
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
        return channel.sendAndReceive(server, "get", serializer.encode(values))
                      .thenApply(a -> null);
    }

    public CompletableFuture< Integer > createTransaction () {
        // TODO
        return CompletableFuture.completedFuture(1);
    }


    public CompletableFuture<List<String>> discoverParticipants() {
        List<String> addresses = new ArrayList<>();
        CompletableFuture<byte[]> response =
                channel.sendAndReceive(coordinator, "discover-participants", serializer.encode(addresses));
        return response.thenApply( bytes -> serializer.decode( bytes ) );
    }

    public CompletableFuture< Void > start () {
        // TODO
        return this.client.start()
                .thenCompose( m -> this.channel.start() )
                .thenApply( a -> null);
    }
}
