package FSD.DistributedTransactions;

import FSD.DistributedTransactions.Coordinator.BaseCoordinator;
import FSD.DistributedTransactions.Coordinator.Coordinator;
import FSD.DistributedTransactions.Coordinator.CoordinatorController;
import FSD.DistributedTransactions.Coordinator.Transaction;
import FSD.DistributedTransactions.Participant.BaseParticipant;
import FSD.DistributedTransactions.Participant.Participant;
import FSD.Logger;
import io.atomix.utils.net.Address;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;

public class Main {
    public static CyclicBarrier transactionBarrier = new CyclicBarrier( 3 );

    public static volatile long transaction = 0;

    public static List< Thread > threads = new ArrayList<>();

    public static void mainParticipant ( Address coordinator, List< Address > participants, int index ) {
        Address address = participants.get( index );

        Participant< Integer > participant = new BaseParticipant< Integer >( address, coordinator, Integer.class );

        try {
            participant.start().get();

            if ( index <= 1 ) {
                Main.transactionBarrier.await();

                Logger.debug( "[MAIN] [%d] Barrier released. Commiting.", index );

                participant.tryCommit( Main.transaction, Arrays.asList( index, 2, 3 ) )
                    .thenRun( () -> {
                        Logger.debug( "[MAIN] [%d] Transaction commited.", index );
                    } );
            }

            System.out.printf( "Participant %d\n", index );
        } catch ( InterruptedException | ExecutionException | BrokenBarrierException e ) {
            e.printStackTrace();
        }
    }

    public static void mainCoordinator ( Address address, List< Address > participants ) {
        Coordinator           coordinator = new BaseCoordinator( participants );
        CoordinatorController controller  = new CoordinatorController( address, coordinator );

        try {
            controller.start().get();

            Transaction tr = coordinator.onTransactionBegin( new int[] { 0, 1 } );

            Main.transaction = tr.id;

            Logger.debug( "[MAIN] Created transaction %d", tr.id );

            Main.transactionBarrier.await();
        } catch ( InterruptedException | ExecutionException | BrokenBarrierException e ) {
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

    public static void main ( String[] args ) {
        Address coordinatorAddress = Address.from( "localhost:12344" );

        List< Address > addresses = Arrays.asList(
                Address.from( "localhost:12345" ),
                Address.from( "localhost:12346" ),
                Address.from( "localhost:12347" )
        );

        Main.run( () -> Main.mainCoordinator( coordinatorAddress, addresses ) );

        for ( int i = 0; i < addresses.size(); i++ ) {
            final int i2 = i;

            Main.run( () -> Main.mainParticipant( coordinatorAddress, addresses, i2 ) );
        }

        Main.join();
    }
}
