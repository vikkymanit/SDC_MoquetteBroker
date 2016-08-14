package de.tum.i13.Exception;

public class QueueEmptyException extends RuntimeException {
    public QueueEmptyException() {
    }

    public QueueEmptyException(String message) {
        super(message);
    }

    public QueueEmptyException(String message, Throwable cause) {
        super(message, cause);
    }
}
