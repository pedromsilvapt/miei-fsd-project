package FSD.DistributedTransactions;

import java.util.ArrayList;
import java.util.List;

public class TransactionRequest {
    private int[] servers;

    public TransactionRequest ( int[] servers ) {
        this.servers = servers;
    }

    public TransactionRequest ( List< Integer > servers ) {
        this.servers = new int[ servers.size() ];

        for ( int i = 0; i < servers.size(); i++ ) {
            this.servers[ i ] = servers.get( i );
        }
    }

    public TransactionRequest () {
        this.servers = new int[ 0 ];
    }

    public List< Integer > getServers () {
        List< Integer > list = new ArrayList<>();

        for ( int i : this.servers ) list.add( i );

        return list;
    }

    public int[] getServersArray () {
        return this.servers;
    }
}
