package de.tum.i13.adaptive;

import java.io.IOException;

public class Temp {
    public static void main(String[] args) throws IOException {
        for (int i = 0; i < 100; i++) {
            System.out.println("difference: " + (System.nanoTime() - System.nanoTime()));
        }
    }
}
