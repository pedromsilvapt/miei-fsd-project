package DistributedTransactions.Participant;

import DistributedTransactions.TransactionReport;
import DistributedTransactions.TransactionState;
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

    public BaseParticipant ( Class< T > type, Address address, Address coordinator ) {
        this.address = address;
        this.coordinator = coordinator;
        this.transactions = new HashMap<>();

        this.serializer = Serializer.builder()
                .withTypes( type )
                .withTypes( TransactionReport.class )
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

        this.sendReport( id, TransactionState.Prepare );

        return tr.future;
    }

    private void writeBlock ( long id, TransactionState state ) {
        SegmentedJournalWriter< LogEntry< T > > writer = this.journal.writer();

        writer.append( new LogEntry<>( id, LogEntryType.fromState( state ) ) );
    }

    private void sendReport ( long id, TransactionState state ) {
        TransactionReport report = new TransactionReport( id, state );

        this.messagingService.sendAsync( this.coordinator, "update-transaction", this.serializer.encode( report ) );
    }

    @Override
    public void onCoordinatorUpdate ( long id, TransactionState state ) {
        Transaction tr = this.transactions.get( id );

        if ( tr == null ) {
            return;
        }

        if ( tr.state == TransactionState.Waiting && state == TransactionState.Prepare ) {
            this.writeBlock( id, TransactionState.Prepare );

            tr.state = state;

            if ( tr.aborted ) {
                this.sendReport( id, TransactionState.Abort );
            } else {
                this.sendReport( id, TransactionState.Commit );
            }
        } else if ( tr.state == TransactionState.Prepare && state == TransactionState.Abort ) {
            this.writeBlock( id, TransactionState.Abort );

            tr.state = state;

            tr.future.complete( false );

            this.transactions.remove( id );
        } else if ( tr.state == TransactionState.Prepare && state == TransactionState.Commit ) {
            this.writeBlock( id, TransactionState.Commit );

            tr.state = state;

            tr.future.complete( true );

            this.transactions.remove( id );
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

            if ( this.transactions.containsKey( entry.id ) ) {
                Transaction tr = new Transaction( entry.id, entry.type.toState() );

                this.transactions.put( entry.id, tr );
            } else {
                Transaction tr = this.transactions.get( entry.id );

                tr.state = entry.type.toState();
            }
        }

        return this.messagingService.start().thenRun( () -> {} );
    }
}
