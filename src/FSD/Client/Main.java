package FSD.Client;

import io.atomix.utils.net.Address;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class Main {
    public static volatile long transaction = 0;

    public static List< Thread > threads = new ArrayList<>();

    public static void mainClient(Address address, Address coordinatorAddress, List<Address> servers) {
        Client client = new BaseClient(address, coordinatorAddress);
        ClientController controller = new ClientController(client, address, coordinatorAddress);

        try {
            controller.start().get();

            // Transaction tr = Get from ...

            // Main.transaction = tr.id;

            // client.get()

            // Logger.debug( "[CLIENT] Got transaction %d", tr.id );

        } catch ( InterruptedException | ExecutionException e ) {
            e.printStackTrace();
        }
    }

    public static void run ( Runnable runnable ) {
        Thread thread = new Thread( runnable );

        Main.threads.add( thread );

        thread.start();
    }

    public static void join () {
        try {
            for ( Thread thread : Main.threads ) thread.join();
        } catch ( InterruptedException ex ) {
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Address clientAddress = Address.from( "localhost:12350" );
        Address coordinatorAddress = Address.from( "localhost:12344" );

        List< Address > addresses = Arrays.asList(
                Address.from( "localhost:12345" ),
                Address.from( "localhost:12346" ),
                Address.from( "localhost:12347" )
        );

        Main.run( () -> Main.mainClient(clientAddress, coordinatorAddress, addresses) );

        Main.join();
    }
}
