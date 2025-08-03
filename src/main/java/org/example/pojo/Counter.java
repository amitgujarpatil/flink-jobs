package org.example.pojo;

public class Counter {
    private int count;

    public synchronized void inc() {
        this.count++;
    }

    public int getCount() {
        return this.count;
    }
}
