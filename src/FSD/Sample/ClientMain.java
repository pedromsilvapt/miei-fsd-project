package FSD.Sample;

import FSD.Client.BaseClient;
import FSD.Client.Client;
import FSD.Client.ClientController;
import io.atomix.utils.net.Address;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ClientMain {
    // USAGE args:
    // <client port> <coordinator port> <action>
    public static void main ( String[] args ) {
        int clientPort      = Integer.parseInt( args[ 0 ] );
        int coordinatorPort = Integer.parseInt( args[ 1 ] );
        int action          = Integer.parseInt( args[ 2 ] );

        Address address            = Address.from( "localhost:" + clientPort );
        Address coordinatorAddress = Address.from(  "localhost:" + coordinatorPort );

        Client           client     = new BaseClient( address, coordinatorAddress );
        ClientController controller = new ClientController( client, address, coordinatorAddress );

        try {
            // Estamos a usar o .get() porque este codigo e apenas de exemplo e como sabemos nao estar mais nada a
            // correr no processo, torna mais perceptivel o que esta a fazer
            controller.start().get();

            // PUT
            if ( action == 1 ) {
                Map< Long, byte[] > map = new HashMap<>();

                map.put( 1L, "Teste 1".getBytes() );
                map.put( 2L, "Teste 2".getBytes() );

                System.out.println( client.put( map ).get() );
            } else if ( action == 2 ) {
                Map< Long, byte[] > map = new HashMap<>();

                map.put( 2L, "Teste 2.1".getBytes() );
                map.put( 3L, "Teste 3.1".getBytes() );

                System.out.println( client.put( map ).get() );
            } else if ( action == 3 ) {
                Collection<Long> keys = Arrays.asList( 1L, 2L, 3L );

                System.out.println( client.get( keys ).get() );
            }
        } catch ( InterruptedException | ExecutionException e ) {
            e.printStackTrace();
        }
    }
}
