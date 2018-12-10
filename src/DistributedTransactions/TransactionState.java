package DistributedTransactions;

public enum TransactionState {
    Waiting,
    Prepare,
    Commit,
    Abort
}
