package FSD.DistributedTransactions.Coordinator;

import FSD.DistributedTransactions.TransactionState;
import FSD.Logger;
import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.storage.journal.SegmentedJournalReader;
import io.atomix.storage.journal.SegmentedJournalWriter;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class BaseCoordinator implements Coordinator {
    private Consumer< Transaction >  transactionListener = null;
    private Map< Long, Transaction > transactions        = new HashMap<>();
    private long                     sequentialId        = 0;

    private List< Address >              serverAddresses;
    private SegmentedJournal< LogEntry > journal;
    private Serializer                   serializer;

    public BaseCoordinator ( List< Address > servers ) {
        this.serverAddresses = servers;

        this.serializer = Serializer.builder()
                .withTypes( LogEntry.class )
                .withTypes( TransactionState.class )
                .build();

        this.journal = SegmentedJournal.< LogEntry >builder()
                .withName( "coordinator" )
                .withSerializer( serializer )
                .build();
    }

    public void setTransactionListener ( Consumer<Transaction> listener ) {
        this.transactionListener = listener;
    }

    public Address getServer ( int index ) {
        return this.serverAddresses.get( index );
    }

    @Override
    public List< Address > getServersList () {
        return this.serverAddresses;
    }

    public int getServerIndex ( Address address ) {
        for ( int i = 0; i < this.serverAddresses.size(); i++ ) {
            if ( this.serverAddresses.get( i ).equals( address ) ) {
                return i;
            }
        }

        return -1;
    }

    public Collection< Transaction > getTransactions () {
        return this.transactions.values();
    }

    public void writeBlock ( long id, TransactionState state ) {
        writeBlock( id, state, null );
    }

    private void writeBlock ( long id, TransactionState state, int servers[] ) {
        SegmentedJournalWriter< LogEntry > writer = this.journal.writer();

        writer.append( new LogEntry( id, servers, state ) );

        writer.flush();

        writer.close();
    }

    @Override
    public Transaction onTransactionBegin ( int[] servers ) {
        Transaction transaction = new Transaction( this.sequentialId++, servers );

        Logger.debug( "[COORDINATOR] Beginning transaction %s", transaction );

        this.writeBlock( transaction.id, transaction.globalState, transaction.servers );

        this.transactions.put( transaction.id, transaction );

        return transaction;
    }

    @Override
    public void onServerUpdate ( long id, int server, TransactionState state ) throws Exception {
        Transaction tr = this.transactions.get( id );

        if ( tr == null ) {
            throw new Exception( "No transaction with id " + id + " was found." );
        }

        Logger.debug( "[COORDINATOR] Setting transaction %d server %d from state %s to %s", id, server, tr.getServerState( server ), state );

        tr.setServerState( server, state );

        this.onStateChange( tr );
    }

    @Override
    public void onTransactionPrepared ( long id ) {

    }

    @Override
    public void onTransactionFulfilled ( long id, TransactionState state ) {

    }

    public void onStateChange ( Transaction transaction ) {
        TransactionState previousState = transaction.globalState;

        if ( transaction.globalState == TransactionState.Waiting ) {
            if ( transaction.allState( TransactionState.Prepare ) ) {
                transaction.globalState = TransactionState.Commit;
            } else if ( transaction.anyState( TransactionState.Abort ) ) {
                transaction.globalState = TransactionState.Abort;
            }
        } else if ( transaction.globalState == TransactionState.Commit || transaction.globalState == TransactionState.Abort ) {
            if ( transaction.allState( TransactionState.Commit ) || transaction.allState( TransactionState.Abort ) ) {
                this.transactions.remove( transaction.id );
            }
        }

        if ( transaction.globalState != previousState ) {
            Logger.debug( "[COORDINATOR] Setting transaction %d global state from %s to %s", transaction.id, previousState, transaction.globalState );

            this.writeBlock( transaction.id, transaction.globalState );

            if ( this.transactionListener != null ) {
                this.transactionListener.accept( transaction );
            }
        }
    }

    @Override
    public CompletableFuture< Void > start () {
        SegmentedJournalReader< LogEntry > reader = this.journal.openReader( 0 );

        for ( SegmentedJournalReader< LogEntry > it = reader; it.hasNext(); ) {
            LogEntry entry = it.next().entry();

            Logger.debug( "[COORDINATOR] Reading entry %s", entry );

            if ( !this.transactions.containsKey( entry.tid ) ) {
                Transaction tr = new Transaction( entry.tid, entry.servers );

                tr.globalState = entry.type;

                this.transactions.put( entry.tid, tr );
            } else {
                Transaction tr = this.transactions.get( entry.tid );

                tr.globalState = entry.type;
            }

            this.sequentialId = entry.tid + 1;
        }

        Logger.debug( "[COORDINATOR] Initial transaction id: %d", this.sequentialId );

        return CompletableFuture.completedFuture( null );
    }
}
