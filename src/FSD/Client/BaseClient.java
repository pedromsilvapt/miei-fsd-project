package FSD.Client;

import io.atomix.utils.net.Address;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class BaseClient implements Client {
    private long transactionId;
    private Address address;
    private Address coordinatorAddress;
    private ClientController controller;

    public BaseClient(Address address, Address coordinatorAddress) {
        this.address = address;
        this.coordinatorAddress = coordinatorAddress;
    }

    public long getTransactionId() {
        return transactionId;
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
        return controller.putRequest(transactionId, values);
    }

    @Override
    public CompletableFuture<Map<Long, byte[]>> get(Collection<Long> values) {
        // TODO
        return null;
    }

    @Override
    public CompletableFuture<Void> start() {
        // TODO: Start transaction and get ID
        return null;
    }
}
