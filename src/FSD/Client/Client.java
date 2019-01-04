package FSD.Client;

import FSD.Controllable;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface Client extends Controllable< ClientController > {
    CompletableFuture<Boolean> put(Map<Long, byte[]> values);
    CompletableFuture<Map<Long, byte[]>> get(Collection<Long> values);
    CompletableFuture< Void > start ();
}
