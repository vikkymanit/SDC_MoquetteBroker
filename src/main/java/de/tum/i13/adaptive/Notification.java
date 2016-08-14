package de.tum.i13.adaptive;

public class Notification {
    private final Long arrivalTime;
    private final byte[] payload;

    public Notification(Long arrivalTime, byte[] payload) {
        this.arrivalTime = arrivalTime;
        this.payload = payload;
    }

    public Long getArrivalTime() {
        return arrivalTime;
    }

    public byte[] getPayload() {
        return payload;
    }
}
