package DistributedTransactions.Coordinator;

import DistributedTransactions.TransactionState;
import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.storage.journal.SegmentedJournalReader;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class BaseCoordinator implements Coordinator {
    private Map< Long, Transaction >     transactions    = new HashMap<>();
    private List< Address >              serverAddresses = new ArrayList<>();
    private long                         sequentialId    = 0;
    private SegmentedJournal< LogEntry > journal         = null;
    private Serializer serializer;

    public BaseCoordinator ( List< Address > servers ) {
        this.serverAddresses = servers;

        this.serializer = Serializer.builder()
                .withTypes( LogEntry.class )
                .build();

        this.journal = SegmentedJournal.< LogEntry >builder()
                .withName( "coordinator" )
                .withSerializer( serializer )
                .build();
    }

    public Address getServer ( int index ) {
        return this.serverAddresses.get( index );
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

    @Override
    public Transaction onTransactionBegin ( int[] servers ) {
        Transaction transaction = new Transaction( this.sequentialId++, servers );

        this.transactions.put( transaction.id, transaction );

        return transaction;
    }

    @Override
    public void onServerUpdate ( long id, int server, TransactionState state ) throws Exception {
        Transaction tr = this.transactions.get( id );

        if ( tr == null ) {
            throw new Exception( "No transaction with id " + id + " was found." );
        }

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
    }

    @Override
    public CompletableFuture< Void > start () {
        SegmentedJournalReader< LogEntry > reader = this.journal.openReader( 0 );

        for ( SegmentedJournalReader< LogEntry > it = reader; it.hasNext(); ) {
            LogEntry entry = it.next().entry();

            if ( this.transactions.containsKey( entry.tid ) ) {
                Transaction tr = new Transaction( entry.tid, entry.servers );

                tr.globalState = entry.type;

                this.transactions.put( entry.tid, tr );
            } else {
                Transaction tr = this.transactions.get( entry.tid );

                tr.globalState = entry.type;
            }

            this.sequentialId = entry.tid + 1;
        }

        return null;
    }
}