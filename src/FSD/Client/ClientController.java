package FSD.Client;

import FSD.DistributedMap.MapNodeController.PutRequest;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ClientController {
    private Address coordinator;
    private Client client;
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

    public CompletableFuture<Boolean> putRequest(long transaction, Map<Long, byte[]> data) {
        PutRequest request = new PutRequest(transaction, data);
        CompletableFuture<byte[]> response =
                channel.sendAndReceive(coordinator, "put", serializer.encode(request));

        return null;
    }
}
