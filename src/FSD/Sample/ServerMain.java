package FSD.Sample;

import FSD.DistributedMap.BaseMapNode;
import FSD.DistributedMap.ChangeLog;
import FSD.DistributedMap.MapNode;
import FSD.DistributedMap.MapNodeController;
import FSD.DistributedTransactions.Coordinator.BaseCoordinator;
import FSD.DistributedTransactions.Coordinator.Coordinator;
import FSD.DistributedTransactions.Coordinator.CoordinatorController;
import FSD.DistributedTransactions.Participant.BaseParticipant;
import FSD.DistributedTransactions.Participant.Participant;
import FSD.Logger;
import io.atomix.utils.net.Address;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class ServerMain {
    // USAGE args:
    // coordinator <port> <participant1 port> <participant2 port> <...>
    // participant <port> <coordinator port>
    public static void main ( String[] args ) {
        String type = args[ 0 ];

        if ( type.equals( "coordinator" ) ) {
            int coordinatorPort = Integer.parseInt( args[ 1 ] );

            Address address = Address.from( "localhost:" + coordinatorPort );

            List< Address > participants = new ArrayList<>();

            for ( int i = 2; i < args.length; i++ ) {
                participants.add( Address.from( "localhost:" + Integer.parseInt( args[ i ] ) ) );
            }

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

            Address address     = Address.from( "localhost:" + participantPort );
            Address coordinator = Address.from( "localhost:" + coordinatorPort );

            Participant< ChangeLog< Long, byte[] > > participant = new BaseParticipant<>( address.toString(), ChangeLog.class );

            MapNode           node       = new BaseMapNode( participant );
            MapNodeController controller = new MapNodeController( node, address, coordinator );

            try {
                controller.start().get();

                Logger.debug( "[MAIN] Participant started at %s.", address.toString() );
            } catch ( InterruptedException | ExecutionException e ) {
                e.printStackTrace();
            }
        }
    }
}
