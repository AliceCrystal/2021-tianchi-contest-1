package com.aliyun.adb.contest.thread;

import com.aliyun.adb.contest.cache.ReadAndCalCache;
import com.aliyun.adb.contest.inner.ReaderAndCalerAndWriter;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class ReadThread implements Runnable {
    // 给每个read thread一个MappedByteBuffer用于读取dataChannel中的数据
    // read thread 每完成一个任务后就会从任务队列中取新的任务
    private MappedByteBuffer mappedByteBuffer;
    private FileChannel dataChannel;
    private ReadAndCalCache[] readAndCalCaches;
    private int cacheId;


    public ReadThread(FileChannel dataChannel, ReadAndCalCache[] readAndCalCaches) {
        this.dataChannel = dataChannel;
        this.readAndCalCaches = readAndCalCaches;
        this.cacheId = 0;
    }

    @Override
    public void run() {
        // 只要有任务线程就不会停
        while (!ReaderAndCalerAndWriter.taskQueue.isEmpty()) {
            try {
                // 从任务队列中拿出一个任务
                long[] piece = ReaderAndCalerAndWriter.taskQueue.take();

                mappedByteBuffer = dataChannel.map(FileChannel.MapMode.READ_ONLY, piece[0], piece[1] - piece[0] + 1);
                System.out.println("Start read segment: [" + piece[0] + ", " + piece[1] + "]");

                while (mappedByteBuffer.remaining() >= ReadAndCalCache.sizeOfByteArray) {
                    // 从emptyQueue中取出一个byte数组
                    if (readAndCalCaches[cacheId].emptyBytesQueue.isEmpty()) {
                        ReaderAndCalerAndWriter.readLA.add(1L);
                    }

                    byte[] bytes = readAndCalCaches[cacheId].emptyBytesQueue.take();

                    // 将数据从磁盘读到byte数组中
                    mappedByteBuffer.get(bytes);

                    // 将byte数组放到fullQueue中
                    readAndCalCaches[cacheId].fullBytesQueue.put(bytes);
                }

                // 但是有一个问题，如果最后mappedByteBuffer剩余的字节数小于bytes.length
                // 调用mappedByteBuffer.get(bytes)会报错，需要做特殊处理
                if (mappedByteBuffer.remaining() > 0) {
                    byte[] bytes = new byte[mappedByteBuffer.remaining()];
                    mappedByteBuffer.get(bytes);
                    readAndCalCaches[cacheId].fullBytesQueue.put(bytes);
                }
                // 向下一个readAndCalCaches写入数据
                cacheId = (cacheId + 1) % readAndCalCaches.length;
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            }
        }
        // 循环结束后，最后传送一个 new byte[]{0}
        try {
            for (int i = 0; i < readAndCalCaches.length; i++) {
                readAndCalCaches[i].fullBytesQueue.put(new byte[]{0});
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("reader " + "done");
        ReaderAndCalerAndWriter.countDownLatch.countDown();
    }
}
