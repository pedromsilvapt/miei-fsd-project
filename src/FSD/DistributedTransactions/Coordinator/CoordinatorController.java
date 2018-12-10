package DistributedTransactions.Coordinator;

import DistributedTransactions.TransactionReport;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CoordinatorController {
    private Coordinator             coordinator;
    private Address                 address;
    private Serializer              serializer;
    private ExecutorService         executorService;
    private ManagedMessagingService channel;


    public CoordinatorController ( Address address, Coordinator coordinator ) {
        this.coordinator = coordinator;

        this.address = address;

        this.serializer = Serializer.builder()
                .withTypes( TransactionReport.class )
                .build();

        this.executorService = Executors.newSingleThreadExecutor();

        this.channel = NettyMessagingService.builder()
                .withAddress( this.address )
                .build();
    }

    public CompletableFuture< Void > start () {
        this.channel.registerHandler( "update-transaction", ( o, m ) -> {
            int server = this.coordinator.getServerIndex( o );

            TransactionReport report = this.serializer.decode( m );

            try {
                this.coordinator.onServerUpdate( report.id, server, report.state );
            } catch ( Exception e ) {
                e.printStackTrace();
            }
        }, this.executorService );

        return this.coordinator.start()
                .thenCompose( m -> this.channel.start() )
                .thenApply( a -> {
                    for ( Transaction tr : this.coordinator.getTransactions() ) {
                        TransactionReport report = new TransactionReport( tr.id, tr.globalState );

                        byte[] message = this.serializer.encode( report );

                        for ( int index : tr.servers ) {
                            Address server = this.coordinator.getServer( index );

                            this.channel.sendAsync( server, "update-transaction", message );
                        }
                    }

                    return null;
                } );
    }
}