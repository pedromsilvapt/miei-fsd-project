package DistributedTransactions.Coordinator;

import DistributedTransactions.TransactionState;

public class Transaction {
    public long             id;
    public int[]            servers;
    public TransactionState serverStates[];
    public TransactionState globalState;

    public Transaction ( long id, int[] servers ) {
        this.id = id;
        this.servers = servers;
        this.serverStates = new TransactionState[ servers.length ];
        this.globalState = TransactionState.Prepare;

        for ( int i = 0; i < this.serverStates.length; i++ ) {
            this.serverStates[ i ] = TransactionState.Waiting;
        }
    }

    public boolean anyState ( TransactionState matched ) {
        for ( TransactionState state : this.serverStates ) {
            if ( state == matched ) {
                return true;
            }
        }

        return false;
    }

    public boolean allState ( TransactionState matched ) {
        for ( TransactionState state : this.serverStates ) {
            if ( state != matched ) {
                return false;
            }
        }

        return true;
    }

    protected int getServerIndex ( int server ) {
        for ( int i = 0; i < this.servers.length; i++ ) {
            if ( this.servers[ i ] == server ) {
                return i;
            }
        }

        return -1;
    }

    public void setServerState ( int server, TransactionState state ) {
        int index = this.getServerIndex( server );

        this.serverStates[ index ] = state;
    }

    public TransactionState getServerState ( int server ) {
        return this.serverStates[ this.getServerIndex( server ) ];
    }
}
