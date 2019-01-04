package FSD.DistributedTransactions.Participant;

import FSD.DistributedMap.MapNodeController;
import FSD.DistributedTransactions.TransactionReport;
import FSD.DistributedTransactions.TransactionRequest;
import FSD.DistributedTransactions.TransactionState;
import FSD.Logger;
import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.storage.journal.SegmentedJournalReader;
import io.atomix.storage.journal.SegmentedJournalWriter;
import io.atomix.utils.serializer.Serializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class BaseParticipant < T > implements Participant< T > {
    private String                            name;
    private Serializer                        serializer;
    private SegmentedJournal< LogEntry< T > > journal;
    private Map< Long, Transaction< T > >     transactions;
    private Class< ? >[]                      serializableTypes;
    private ParticipantController< T >        controller;
    private CompletableFuture< Void > starter = null;

    public BaseParticipant ( String name, Class< ? >... types ) {
        this.name = name;

        this.serializableTypes = types;

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
                .withTypes( types )
                .build();

        this.transactions = new HashMap<>();

        this.journal = SegmentedJournal.< LogEntry< T > >builder()
                .withName( this.name )
                .withSerializer( this.serializer )
                .build();
    }

    @Override
    public Class< ? >[] getSerializableTypes () {
        return serializableTypes;
    }

    @Override
    public ParticipantController< T > getController () {
        return controller;
    }

    @Override
    public void setController ( ParticipantController< T > controller ) {
        this.controller = controller;
    }

    @Override
    public String getName () {
        return name;
    }

    public Collection< Transaction< T > > getTransactions () {
        return this.transactions.values();
    }

    private void writeBlock ( long id, TransactionState state ) {
        SegmentedJournalWriter< LogEntry< T > > writer = this.journal.writer();

        writer.append( new LogEntry<>( id, LogEntryType.fromState( state ) ) );

        writer.flush();

        writer.close();
    }

    @Override
    public CompletableFuture< Boolean > tryCommit ( long id, Collection< T > blocks ) {
        if ( this.transactions.containsKey( id ) ) {
            this.controller.sendReport( id, TransactionState.Prepare );

            return this.transactions.get( id ).future;
        }

        Transaction< T > tr = new Transaction<>( id, TransactionState.Waiting, blocks );

        this.transactions.put( id, tr );

        SegmentedJournalWriter< LogEntry< T > > writer = this.journal.writer();

        for ( T block : blocks ) {
            writer.append( new LogEntry<>( id, LogEntryType.Data, block ) );
        }

        writer.append( new LogEntry< T >( id, LogEntryType.Prepare ) );

        writer.flush();
        writer.close();

        Logger.debug( "[PARTICIPANT][%s] Written to log: Prepare", this.name );

        tr.state = TransactionState.Prepare;

        this.controller.sendReport( id, TransactionState.Prepare );

        return tr.future;
    }

    @Override
    public void onCoordinatorUpdate ( long id, TransactionState state ) {
        Logger.debug( "[PARTICIPANT] [%s] onCoordinatorUpdate: %d, %s", this.name, id, state );

        Transaction< T > tr = this.transactions.get( id );

        if ( tr == null ) {
            Logger.debug( "[PARTICIPANT] [%s] onCoordinatorUpdate: Transaction %d not found", this.name, id );

            return;
        }

        Logger.debug( "[PARTICIPANT] [%s] onCoordinatorUpdate: Transaction %s", this.name, tr );

        if ( tr.state == TransactionState.Prepare && state == TransactionState.Prepare ) {
            this.controller.sendReport( id, TransactionState.Prepare );
        } else if ( state == TransactionState.Abort ) {
            this.writeBlock( id, TransactionState.Abort );

            Logger.debug( "[PARTICIPANT] [%s] onCoordinatorUpdate: %d aborting", this.name, id );

            this.transactions.remove( id );

            tr.state = state;

            tr.future.complete( false );
        } else if ( tr.state == TransactionState.Prepare && state == TransactionState.Commit ) {
            this.writeBlock( id, TransactionState.Commit );

            Logger.debug( "[PARTICIPANT] [%s] onCoordinatorUpdate: %d commiting", this.name, id );

            this.transactions.remove( id );

            tr.state = state;

            tr.future.complete( true );
        } else {
            Logger.debug( "[PARTICIPANT] [%s] onCoordinatorUpdate: %d no matching state for %s", this.name, id, tr.state );
        }
    }

    @Override
    public void release ( long transaction ) {
        // TODO As of now, all transactions, once recorded in the log, are kept there forever
        // This is wasteful, since once those transactions have been persisted to disk, they can be released
    }

    @Override
    public CompletableFuture< Void > abort ( long id ) {
        Transaction< T > tr = null;

        if ( this.transactions.containsKey( id ) ) {
            tr = this.transactions.get( id );
        } else {
            tr = new Transaction<>( id, TransactionState.Abort );

            this.transactions.put( id, tr );
        }

        writeBlock( id, TransactionState.Abort );

        Logger.debug( "[PARTICIPANT][%s] Written to log: Abort", this.name );

        tr.state = TransactionState.Abort;

        this.controller.sendReport( id, TransactionState.Abort );

        return tr.future.thenRun( () -> {} );
    }

    @Override
    public CompletableFuture< Void > start () {
        if ( this.starter != null ) {
            return this.starter;
        }

        SegmentedJournalReader< LogEntry< T > > reader = this.journal.openReader( 0 );

        for ( SegmentedJournalReader< LogEntry< T > > it = reader; it.hasNext(); ) {
            LogEntry< T > entry = it.next().entry();

            if ( !this.transactions.containsKey( entry.id ) ) {
                Transaction< T > tr = new Transaction<>( entry.id, entry.type.toState() );

                if ( entry.type == LogEntryType.Data ) {
                    tr.blocks.add( entry.data );
                }

                this.transactions.put( entry.id, tr );
            } else {
                Transaction< T > tr = this.transactions.get( entry.id );

                Logger.debug( "[PARTICIPANT] [%s] start: type = %s", this.name, entry.type );

                tr.state = entry.type.toState();

                if ( entry.type == LogEntryType.Data ) {
                    tr.blocks.add( entry.data );
                }

                if ( tr.state == TransactionState.Commit || tr.state == TransactionState.Abort ) {
                    this.transactions.remove( tr.id );
                }
            }
        }

        return this.starter = CompletableFuture.completedFuture( null );
    }
}
