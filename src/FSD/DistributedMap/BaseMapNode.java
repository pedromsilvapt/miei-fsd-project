package FSD.DistributedMap;

import FSD.DistributedTransactions.Participant.BaseParticipant;
import FSD.DistributedTransactions.Participant.Participant;
import FSD.DistributedTransactions.Participant.Transaction;
import FSD.DistributedTransactions.TransactionState;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class BaseMapNode implements MapNode {
    private Participant< ChangeLog< Long, byte[] > > transactions;
    private Map< Long, byte[] >       data       = new HashMap<>();
    private Map< Long, Long >         locks      = new HashMap<>();
    private MapNodeController         controller = null;
    private CompletableFuture< Void > starter    = null;
    private MapNodeStorage            storage    = null;

    @Override
    public MapNodeController getController () {
        return controller;
    }

    @Override
    public void setController ( MapNodeController controller ) {
        this.controller = controller;
    }

    @Override
    public Participant< ChangeLog< Long, byte[] > > getTransactions () {
        return this.transactions;
    }

    public BaseMapNode ( String name ) {
        this( new BaseParticipant<>( name, ChangeLog.class, Long.class ) );
    }

    public BaseMapNode ( Participant< ChangeLog< Long, byte[] > > participant ) {
        this.transactions = participant;

        this.storage = new MapNodeStorage( this.transactions.getName() );
    }

    private boolean lock ( long key, long transaction ) {
        Long locker = this.locks.get( key );

        if ( locker == null ) {
            this.locks.put( key, transaction );

            return true;
        }

        return false;
    }

    private boolean lockAll ( Collection< Long > keys, long transaction ) {
        Set< Long > locked = new HashSet<>();

        for ( Long key : keys ) {
            if ( this.lock( key, transaction ) ) {
                locked.add( key );
            } else {
                this.unlockAll( locked, transaction );

                return false;
            }
        }

        return true;
    }

    private void unlock ( long key, long transaction ) {
        if ( this.locks.get( key ) == transaction ) {
            this.locks.remove( key );
        }
    }

    private void unlockAll ( Collection< Long > keys, long transaction ) {
        for ( Long key : keys ) {
            this.unlock( key, transaction );
        }
    }

    @Override
    public CompletableFuture< Boolean > put ( long transaction, Map< Long, byte[] > data ) {
        List< ChangeLog< Long, byte[] > > blocks = new ArrayList<>();

        for ( Long key : data.keySet() ) {
            blocks.add( new ChangeLog<>( key, this.data.get( key ), data.get( key ) ) );
        }

        if ( !this.lockAll( data.keySet(), transaction ) ) {
            return CompletableFuture.completedFuture( false );
        }

        return this.transactions.tryCommit( transaction, blocks ).thenApply( commited -> {
            this.unlockAll( data.keySet(), transaction );

            this.data.putAll( data );

            try {
                this.storage.putAll( data );
            } catch ( IOException e ) {
                e.printStackTrace();
            }

            this.transactions.release( transaction );

            return commited;
        } );
    }

    @Override
    public CompletableFuture< Map< Long, byte[] > > get ( Collection< Long > keys ) {
        Map< Long, byte[] > values = new HashMap<>();

        for ( Long key : keys ) {
            values.put( key, this.data.get( key ) );
        }

        return CompletableFuture.completedFuture( values );
    }

    public CompletableFuture< Void > start () {
        if ( this.starter != null ) {
            return this.starter;
        }

        return this.starter = this.transactions.start().thenRun( () -> {
            this.data = this.storage.getAll();

            for ( Transaction<ChangeLog<Long, byte[]>> transaction : this.transactions.getTransactions() ) {
                try {
                    if ( transaction.state == TransactionState.Commit ) {
                        for ( ChangeLog<Long, byte[]> change : transaction.blocks ) {
                            this.data.put( change.getKey(), change.getNewValue() );
                            this.storage.put( change.getKey(), change.getNewValue() );
                        }

                        // RELEASE THIS TRANSACTION
                        this.transactions.release( transaction.id );
                    } else if (transaction.state == TransactionState.Abort ) {
                        for ( ChangeLog<Long, byte[]> change : transaction.blocks ) {
                            this.data.put( change.getKey(), change.getOldValue() );
                            this.storage.put( change.getKey(), change.getOldValue() );
                        }

                        // RELEASE THIS TRANSACTION
                        this.transactions.release( transaction.id );
                    }
                } catch ( IOException ignored ) { }
            }
        } );
    }
}
