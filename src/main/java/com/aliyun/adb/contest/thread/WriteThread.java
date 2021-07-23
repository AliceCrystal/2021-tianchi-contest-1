package com.aliyun.adb.contest.thread;

import com.aliyun.adb.contest.cache.ByteBufferCache;
import com.aliyun.adb.contest.cache.CalAndWriteCache;
import com.aliyun.adb.contest.core.ReaderAndCalerAndWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class WriteThread implements Runnable{
    public int threadId;
    public CalAndWriteCache calAndWriteCache;
    public FileChannel[][][] fileChannels;
    public ByteBufferCache byteBufferCache;

    public WriteThread(int threadId, CalAndWriteCache calAndWriteCache, FileChannel[][][] fileChannels){
        this.threadId = threadId;
        this.calAndWriteCache = calAndWriteCache;
        this.fileChannels = fileChannels;
    }

    // 那么问题来了，写线程应该什么时候结束呢？目前尝试用计数的方式解决
    @Override
    public void run() {
        while(true){
            try {
                byteBufferCache = calAndWriteCache.get();
                if(byteBufferCache.colIndex == -1) {
                    break;
                }
                ByteBuffer byteBuffer = byteBufferCache.fullBufferQueue.take();

                byteBuffer.flip();
                fileChannels[byteBufferCache.colIndex][byteBufferCache.bucketIndex][threadId].write(byteBuffer);
                byteBuffer.clear();

                // byteBuffer使用完之后还需要回收
                byteBufferCache.emptyBufferQueue.put(byteBuffer);

            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            }
        }

        System.out.println("writer-" + threadId + " done.");
        ReaderAndCalerAndWriter.countDownLatch.countDown();
    }
}
