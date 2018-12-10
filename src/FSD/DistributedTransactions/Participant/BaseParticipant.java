package FSD.DistributedTransactions.Participant;

import FSD.DistributedTransactions.TransactionReport;
import FSD.DistributedTransactions.TransactionState;
import FSD.Logger;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.storage.journal.SegmentedJournalReader;
import io.atomix.storage.journal.SegmentedJournalWriter;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BaseParticipant < T > implements Participant< T > {
    private Serializer                        serializer;
    private ExecutorService                   executorService;
    private ManagedMessagingService           messagingService;
    private Address                           address;
    private Address                           coordinator;
    private SegmentedJournal< LogEntry< T > > journal;
    private Map< Long, Transaction >          transactions;

    public BaseParticipant ( Address address, Address coordinator, Class< T > type, Class<Object> ... types ) {
        this.address = address;
        this.coordinator = coordinator;
        this.transactions = new HashMap<>();

        this.serializer = Serializer.builder()
                .withTypes( type )
                .withTypes( types )
                .withTypes( TransactionReport.class )
                .withTypes( TransactionState.class )
                .withTypes( LogEntryType.class )
                .withTypes( LogEntry.class )
                .build();

        this.executorService = Executors.newSingleThreadExecutor();

        this.messagingService = NettyMessagingService
                .builder()
                .withAddress( this.address )
                .build();

        this.journal = SegmentedJournal.< LogEntry< T > >builder()
                .withName( this.address.toString() )
                .withSerializer( this.serializer )
                .build();
    }

    @Override
    public CompletableFuture< Boolean > tryCommit ( long id, Collection< T > blocks ) {
        Transaction tr = new Transaction( id, TransactionState.Waiting );

        this.transactions.put( id, tr );

        SegmentedJournalWriter< LogEntry< T > > writer = this.journal.writer();

        for ( T block : blocks ) {
            writer.append( new LogEntry<>( id, LogEntryType.Data, block ) );
        }

        writer.append( new LogEntry< T >( id, LogEntryType.Prepare ) );

        writer.flush();
        writer.close();

        Logger.debug( "[PARTICIPANT][%s] Written to log: Prepare", this.address );

        tr.state = TransactionState.Prepare;

        this.sendReport( id, TransactionState.Prepare );

        return tr.future;
    }

    private void writeBlock ( long id, TransactionState state ) {
        SegmentedJournalWriter< LogEntry< T > > writer = this.journal.writer();

        writer.append( new LogEntry<>( id, LogEntryType.fromState( state ) ) );

        writer.flush();

        writer.close();
    }

    private void sendReport ( long id, TransactionState state ) {
        TransactionReport report = new TransactionReport( id, state );

        Logger.debug( "[PARTICIPANT][%s] Sending report to coordinator: %s", this.address, report );

        this.messagingService.sendAsync( this.coordinator, "update-transaction", this.serializer.encode( report ) );
    }

    @Override
    public void onCoordinatorUpdate ( long id, TransactionState state ) {
        Logger.debug( "[PARTICIPANT][%s] onCoordinatorUpdate: %d, %s", this.address, id, state );

        Transaction tr = this.transactions.get( id );

        if ( tr == null ) {
            Logger.debug( "[PARTICIPANT][%s] onCoordinatorUpdate: Transaction %d not found", this.address, id );

            return;
        }

        Logger.debug( "[PARTICIPANT] [%s] onCoordinatorUpdate: Transaction %s", this.address, tr );

        if ( tr.state == TransactionState.Prepare && state == TransactionState.Prepare ) {
            this.sendReport( id, TransactionState.Prepare );
        } else if ( tr.state == TransactionState.Prepare && state == TransactionState.Abort ) {
            this.writeBlock( id, TransactionState.Abort );

            Logger.debug( "[PARTICIPANT] [%s] onCoordinatorUpdate: %d aborting", this.address, id );

            this.transactions.remove( id );

            tr.state = state;

            tr.future.complete( false );
        } else if ( tr.state == TransactionState.Prepare && state == TransactionState.Commit ) {
            this.writeBlock( id, TransactionState.Commit );

            Logger.debug( "[PARTICIPANT] [%s] onCoordinatorUpdate: %d commiting", this.address, id );

            this.transactions.remove( id );

            tr.state = state;

            tr.future.complete( true );
        } else {
            Logger.debug( "[PARTICIPANT] [%s] onCoordinatorUpdate: %d no matching state for %s", this.address, id, tr.state );
        }
    }

    @Override
    public void release ( long transaction ) {

    }

    @Override
    public CompletableFuture< Void > start () {
        SegmentedJournalReader< LogEntry<T> > reader = this.journal.openReader( 0 );

        for ( SegmentedJournalReader< LogEntry<T> > it = reader; it.hasNext(); ) {
            LogEntry entry = it.next().entry();

            if ( !this.transactions.containsKey( entry.id ) ) {
                Transaction tr = new Transaction( entry.id, entry.type.toState() );

                this.transactions.put( entry.id, tr );
            } else {
                Transaction tr = this.transactions.get( entry.id );

                Logger.debug( "[PARTICIPANT] [%s] start: type = %s", this.address, entry.type );

                tr.state = entry.type.toState();

                if ( tr.state == TransactionState.Commit || tr.state == TransactionState.Abort ) {
                    this.transactions.remove( tr.id );
                }
            }
        }

        this.messagingService.registerHandler( "update-transaction", (o, m) -> {
            TransactionReport report = this.serializer.decode( m );

            this.onCoordinatorUpdate( report.id, report.state );
        }, this.executorService );

        return this.messagingService.start().thenRun( () -> {} );
    }
}
