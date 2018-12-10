package FSD.DistributedTransactions.Participant;

import FSD.DistributedTransactions.TransactionState;

enum LogEntryType {
    Data,
    Prepare,
    Commit,
    Abort;

    public TransactionState toState () {
        switch ( this ) {
            case Prepare:
                return TransactionState.Prepare;
            case Commit:
                return TransactionState.Commit;
            case Abort:
                return TransactionState.Abort;
            case Data:
                return TransactionState.Waiting;
        }

        return null;
    }

    public static LogEntryType fromState ( TransactionState state ) {
        switch ( state ) {
            case Prepare:
                return LogEntryType.Prepare;
            case Commit:
                return LogEntryType.Commit;
            case Abort:
                return LogEntryType.Abort;
            default:
                return null;
        }
    }
}
