package com.aliyun.adb.contest.cache;

import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;

public class ByteBufferCache {
    public int colIndex;
    public int bucketIndex;
    public static final int numOfByteBuffer = 8;
    public static final int sizeOfByteBuffer = 1024;
    public ByteBuffer currentBuffer;
    public LinkedBlockingQueue<ByteBuffer> emptyBufferQueue;
    public LinkedBlockingQueue<ByteBuffer> fullBufferQueue;

    public ByteBufferCache(int colIndex, int bucketIndex){
        this.colIndex = colIndex;
        this.bucketIndex = bucketIndex;
        emptyBufferQueue = new LinkedBlockingQueue<>();
        fullBufferQueue = new LinkedBlockingQueue<>();

        for (int i = 0; i < numOfByteBuffer; i++) {
            emptyBufferQueue.add(ByteBuffer.allocateDirect(sizeOfByteBuffer));
        }
    }

    public void updateCurrentByteBuffer() throws InterruptedException {
        this.currentBuffer = emptyBufferQueue.take();
    }

    public void toFullBufferQueue(){
        fullBufferQueue.add(currentBuffer);
        currentBuffer = null;
    }
}
