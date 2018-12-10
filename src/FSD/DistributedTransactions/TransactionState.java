package FSD.DistributedTransactions;

public enum TransactionState {
    Waiting,
    Prepare,
    Commit,
    Abort
}
