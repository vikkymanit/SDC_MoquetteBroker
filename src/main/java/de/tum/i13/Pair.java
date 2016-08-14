package de.tum.i13;

public class Pair<X, Y> {
    public final X x;
    public final Y y;

    public Pair(X x, Y y) {
        this.x = x;
        this.y = y;
    }

    public Y getValue() {
        return this.y;
    }

    public X getKey() {
        return this.x;
    }
}