package org.example.record;

public class Block {
    Record records[];

    public Block() {
        records = new Record[BlockingFactor.VAL];
    }
}
