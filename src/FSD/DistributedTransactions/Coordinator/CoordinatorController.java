package FSD.DistributedTransactions.Coordinator;

import FSD.DistributedMap.MapNodeController;
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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class CoordinatorController {
    private Coordinator             coordinator;
    private Address                 address;
    private Serializer              serializer;
    private ExecutorService         executorService;
    private ManagedMessagingService channel;


    public CoordinatorController ( Address address, Coordinator coordinator ) {
        this.coordinator = coordinator;

        this.coordinator.setController( this );

        this.address = address;

        this.serializer = Serializer.builder()
                .withTypes( MapNodeController.PutRequest.class )
                .withTypes( ArrayList.class )
                .withTypes( TransactionRequest.class )
                .withTypes( TransactionReport.class )
                .withTypes( TransactionState.class )
                .withTypes( TransactionReport.class )
                .withTypes( TransactionState.class )
                .withTypes( LogEntryType.class )
                .withTypes( LogEntry.class )
                .build();

        this.executorService = Executors.newSingleThreadExecutor();

        this.channel = NettyMessagingService.builder()
                .withAddress( this.address )
                .build();
    }

    public void onTransactionChange ( Transaction tr ) {
        TransactionReport report = new TransactionReport( tr.id, tr.globalState );

        byte[] data = this.serializer.encode( report );

        Logger.debug( "[COORDINATOR] Sending report %s to %s.", report, Arrays.toString( tr.servers ) );

        for ( int index : tr.servers ) {
            Logger.debug( "[COORDINATOR] %s.", this.coordinator.getServer( index ) );

            this.channel.sendAsync( this.coordinator.getServer( index ), "update-transaction", data );
        }
    }

    public CompletableFuture< Void > start () {
        this.channel.registerHandler( "update-transaction", ( o, m ) -> {
            int server = this.coordinator.getServerIndex( o );

            TransactionReport report = this.serializer.decode( m );

            try {
                this.coordinator.onServerUpdate( report.id, server, report.state );
            } catch ( Exception e ) {
                Logger.error( e );
            }
        }, this.executorService );

        this.channel.registerHandler( "discover-participants", ( o, m ) -> {
            List< String > addresses = this.coordinator.getServersList()
                    .stream()
                    .map( Address::toString )
                    .collect( Collectors.toList() );

            return this.serializer.encode( addresses );
        }, this.executorService );

        this.channel.registerHandler( "create-transaction", ( o, m ) -> {
            TransactionRequest request = this.serializer.decode( m );

            Transaction tr = this.coordinator.onTransactionBegin( request.getServersArray() );

            return CompletableFuture.completedFuture( this.serializer.encode( tr.id ) );
        } );

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