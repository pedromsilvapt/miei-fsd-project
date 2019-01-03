package FSD.DistributedTransactions.Participant;

import FSD.DistributedTransactions.TransactionReport;
import FSD.DistributedTransactions.TransactionState;
import FSD.Logger;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ParticipantController < T > {
    private Address                 coordinator;
    private Participant< T >        participant;
    private Serializer              serializer;
    private ExecutorService         executorService;
    private ManagedMessagingService messagingService;
    private CompletableFuture< Void > starter = null;

    public ParticipantController ( Participant< T > participant, Address address, Address coordinator ) {
        this.coordinator = coordinator;

        this.participant = participant;

        this.participant.setController( this );

        this.serializer = Serializer.builder()
                .withTypes( this.participant.getSerializableTypes() )
                .withTypes( TransactionReport.class )
                .withTypes( TransactionState.class )
                .withTypes( LogEntryType.class )
                .withTypes( LogEntry.class )
                .build();

        this.messagingService = NettyMessagingService
                .builder()
                .withAddress( address )
                .build();

        this.executorService = Executors.newSingleThreadExecutor();
    }

    public ParticipantController ( Participant< T > participant, Serializer serializer, ManagedMessagingService messagingService, ExecutorService executorService, Address coordinator ) {
        this.coordinator = coordinator;

        this.participant = participant;

        this.participant.setController( this );

        this.serializer = serializer;

        this.messagingService = messagingService;

        this.executorService = executorService;
    }

    public void sendReport ( long id, TransactionState state ) {
        TransactionReport report = new TransactionReport( id, state );

        Logger.debug( "[PARTICIPANT CONTROLLER] [%s] Sending report to coordinator: %s", this.participant.getName(), report );

        this.messagingService.sendAsync( this.coordinator, "update-transaction", this.serializer.encode( report ) );
    }

    public CompletableFuture< Void > start () {
        if ( this.starter != null ) {
            return this.starter;
        }

        return this.starter = this.participant.start().thenCompose( a -> {
            for ( Transaction<T> transaction : this.participant.getTransactions() ) {
                if ( transaction.state == TransactionState.Waiting ) {
                    this.participant.abort( transaction.id );
                } else if ( transaction.state == TransactionState.Prepare ) {
                    this.participant.tryCommit( transaction.id, null );
                }
            }

            this.messagingService.registerHandler( "update-transaction", ( o, m ) -> {
                TransactionReport report = this.serializer.decode( m );

                this.participant.onCoordinatorUpdate( report.id, report.state );
            }, this.executorService );

            return this.messagingService.start();
        } ).thenRun( () -> {} );
    }
}
