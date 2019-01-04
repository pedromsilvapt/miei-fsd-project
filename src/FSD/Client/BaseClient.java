package FSD.Client;

import io.atomix.utils.net.Address;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class BaseClient implements Client {
    private Address address;
    private Address coordinatorAddress;
    private List< Address > serverAddresses;
    private ClientController controller;

    public BaseClient(Address address, Address coordinatorAddress, List< Address > serverAddresses) {
        this.address = address;
        this.coordinatorAddress = coordinatorAddress;
        this.serverAddresses = serverAddresses;
    }

    @Override
    public ClientController getController () {
        return controller;
    }

    @Override
    public void setController ( ClientController controller ) {
        this.controller = controller;
    }

    @Override
    public CompletableFuture<Boolean> put(Map<Long, byte[]> values) {
        // Edge case: se os valores alterar forem zero, retorna imediatamente (nao vai fazer nada)
        if ( values.size() == 0 ) {
            return CompletableFuture.completedFuture( true );
        }

        return this.createTransaction().thenCompose( transactionId -> {
            // Criamos uma nova completable-future. So vai ser resolvida quando todos os servidores responderem com
            // sucesso, ou quando o primeiro devolver sem sucesso
            CompletableFuture< Boolean > future = new CompletableFuture<>();

            // Distribui as varias chaves pelos servidores correspondentes
            Map< Address, Map< Long, byte[] > > grouped = groupValuesByServer( values );

            // Um contador para saber quantas respostas ainda faltam receber
            AtomicInteger missing = new AtomicInteger( grouped.size() );

            for ( Address address : grouped.keySet() ) {
                // Comunica ao servidor a transaçao
                controller.putRequest( transactionId, address, grouped.get( address ) ).thenAccept( success -> {
                    if ( success ) {
                        int m = missing.decrementAndGet();

                        if ( m == 0 ) {
                            future.complete( true );
                        }
                    } else {
                        if ( missing.get() > 0 ) {
                            // Colocamos o missing a negativo, assim mais nenhuma resposta resposta que possa chegar
                            // mexe com o future.complete
                            missing.set( -1 );

                            future.complete( false );
                        }
                    }
                } );
            }

            return future;
        } );
    }

    @Override
    public CompletableFuture<Map<Long, byte[]>> get(Collection<Long> values) {
        // Criamos uma nova completable-future. So vai ser resolvida quando todos os servidores responderem com
        // sucesso, ou quando o primeiro devolver sem sucesso
        CompletableFuture< Map<Long, byte[]> > future = new CompletableFuture<>();

        Map<Long, byte[]> finalMap = new HashMap<>(  );

        // Distribui as varias chaves pelos servidores correspondentes
        Map< Address, Collection< Long> > grouped = groupKeysByServer( values );

        // Um contador para saber quantas respostas ainda faltam receber
        AtomicInteger missing = new AtomicInteger( grouped.size() );

        for ( Address address : grouped.keySet() ) {
            // Comunica ao servidor a transaçao
            controller.getRequest( address, grouped.get( address ) ).thenAccept( map -> {
                finalMap.putAll( map );

                int m = missing.decrementAndGet();

                if ( m == 0 ) {
                    future.complete( finalMap );
                }
            } );
        }

        return future;
    }

    public CompletableFuture< Integer > createTransaction () {
        return this.controller.createTransaction();
    }

    public Map< Address, Map< Long, byte[] > > groupValuesByServer ( Map< Long, byte[] > values ) {
        Map< Address, Map< Long, byte[] > > grouped = new HashMap<>();

        for ( Long key : values.keySet() ) {
            Address addr = this.serverAddresses.get( ( int ) ( key % this.serverAddresses.size() ) );

            if ( !grouped.containsKey( addr ) ) {
                grouped.put( addr, new HashMap<>() );
            }

            grouped.get( addr ).put( key, values.get( key ) );
        }

        return grouped;
    }

    public Map< Address, Collection< Long > > groupKeysByServer ( Collection< Long > keys ) {
        Map< Address, Collection< Long > > grouped = new HashMap<>();

        for ( Long key : keys ) {
            Address addr = this.serverAddresses.get( ( int ) ( key % this.serverAddresses.size() ) );

            if ( !grouped.containsKey( addr ) ) {
                grouped.put( addr, new ArrayList<>() );
            }

            grouped.get( addr ).add( key );
        }

        return grouped;
    }

    @Override
    public CompletableFuture<Void> start() {
        return controller.discoverParticipants()
                .thenAccept( participants ->
                        serverAddresses = participants
                        .stream()
                        .map(Address::from)
                        .collect(Collectors.toList())
                );
    }
}
