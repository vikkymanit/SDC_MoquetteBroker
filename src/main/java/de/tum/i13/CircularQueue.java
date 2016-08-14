package de.tum.i13;

import de.tum.i13.Exception.QueueEmptyException;

import java.util.ArrayList;

public class CircularQueue {
    private ArrayList<byte[]> array;

    private final int count;

    private int insertionCount = 0;

    private int front = 0, rear = 0;

    public CircularQueue(int count) {
        this.count = count;
        array = new ArrayList<>(count);
        array.ensureCapacity(count);
        for (int i = 0; i < count; i++) {
            array.add(new byte[0]);
        }
    }

    public int size() {
        return (rear >= front) ? (rear - front) : (count - (front - rear));
    }

    public boolean isEmpty() {
        return rear == front;
    }

    public boolean isFull() {
        int difference = rear - front;
        return difference == -1 || difference == count;
    }

    public void enqueue(byte[] item) {
        insertionCount++;
        if (isFull()) {
            dequeue();
//            throw new de.tum.i13.Exception.QueueFullException("Queue is Full");
        }

        array.set(rear, item);
        rear = (rear + 1) % count;
    }

    public byte[] dequeue() {
        byte[] item;
        if (isEmpty()) {
            throw new QueueEmptyException("Queue is Empty");
        } else {
            item = array.get(front);
            array.set(front, null);
            front = (front + 1) % count;
        }
        return item;
    }

    public ArrayList<byte[]> getAll() {
        if (insertionCount < count) {
            ArrayList<byte[]> temp = new ArrayList<>(insertionCount);
            for (int i = front; i < this.count; i++) {
                temp.add(array.get(i));
            }
            return temp;
        }

        return array;
    }
}
