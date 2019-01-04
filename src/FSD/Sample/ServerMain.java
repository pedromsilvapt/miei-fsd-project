package FSD.Sample;

import FSD.DistributedTransactions.Coordinator.BaseCoordinator;
import FSD.DistributedTransactions.Coordinator.Coordinator;
import FSD.DistributedTransactions.Coordinator.CoordinatorController;
import FSD.DistributedTransactions.Coordinator.Transaction;
import FSD.DistributedTransactions.Main;
import FSD.DistributedTransactions.Participant.BaseParticipant;
import FSD.DistributedTransactions.Participant.Participant;
import FSD.DistributedTransactions.Participant.ParticipantController;
import FSD.Logger;
import io.atomix.utils.net.Address;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;

public class ServerMain {
    // USAGE args:
    // coordinator <port> <participant1 port> <participant2 port> <...>
    // participant <port> <coordinator port>
    public static void main ( String[] args ) {
        String type = args[ 0 ];

        if ( type.equals( "coordinator" ) ) {
            int coordinatorPort = Integer.parseInt( args[ 1 ] );

            Address address = Address.from( coordinatorPort );

            List< Address > participants = new ArrayList<>();

            for ( int i = 2; i < args.length; i++ ) participants.add( Address.from( Integer.parseInt( args[ i ] ) ) );

            Coordinator           coordinator = new BaseCoordinator( participants );
            CoordinatorController controller  = new CoordinatorController( address, coordinator );

            try {
                controller.start().get();

                Logger.debug( "[MAIN] Coordinator started at %s", address.toString() );
            } catch ( InterruptedException | ExecutionException e ) {
                e.printStackTrace();
            }
        } else if ( type.equals( "participant" ) ) {
            int participantPort = Integer.parseInt( args[ 1 ] );
            int coordinatorPort = Integer.parseInt( args[ 2 ] );

            Address address = Address.from( participantPort );
            Address coordinator = Address.from( coordinatorPort );

            Participant< Integer >         participant = new BaseParticipant< Integer >( address.toString(), Integer.class );
            ParticipantController<Integer> controller  = new ParticipantController<>( participant, address, coordinator );

            try {
                controller.start().get();

                Logger.debug( "[MAIN] Participant started at %s.", address.toString() );
            } catch ( InterruptedException | ExecutionException e ) {
                e.printStackTrace();
            }
        }
    }
}
