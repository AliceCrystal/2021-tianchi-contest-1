package com.aliyun.adb.contest.cache;

import com.aliyun.adb.contest.inner.ReaderAndCalerAndWriter;

import java.util.concurrent.LinkedBlockingQueue;

// 计算线程和写线程之间只有一个缓存块，而且存的是ByteBufferCache引用
public class CalAndWriteCache {
    public LinkedBlockingQueue<ByteBufferCache> ByteBufferCacheQueue;

    public CalAndWriteCache(){
        ByteBufferCacheQueue = new LinkedBlockingQueue<>();
    }

    public void put(ByteBufferCache byteBufferCache) throws InterruptedException {
        ByteBufferCacheQueue.put(byteBufferCache);
    }

    public boolean isEmpty() {
        return ByteBufferCacheQueue.isEmpty();
    }

    public ByteBufferCache get() throws InterruptedException {
        if(ByteBufferCacheQueue.isEmpty()) ReaderAndCalerAndWriter.writeLA.add(1L);
        return ByteBufferCacheQueue.take();
    }
}
