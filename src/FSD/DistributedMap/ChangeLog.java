package FSD.DistributedMap;

public class ChangeLog<K, V> {
    private K key;
    private V oldValue;
    private V newValue;

    public ChangeLog ( K key, V value ) {
        this( key, null, value );
    }

    public ChangeLog ( K key, V oldValue, V newValue ) {
        this.key = key;
        this.oldValue = oldValue;
        this.newValue = newValue;
    }

    public K getKey () {
        return key;
    }

    public V getNewValue () {
        return newValue;
    }

    public V getOldValue () {
        return oldValue;
    }

    public void setKey ( K key ) {
        this.key = key;
    }

    public void setNewValue ( V newValue ) {
        this.newValue = newValue;
    }

    public void setOldValue ( V oldValue ) {
        this.oldValue = oldValue;
    }
}
