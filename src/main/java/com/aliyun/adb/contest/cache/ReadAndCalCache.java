package com.aliyun.adb.contest.cache;

import java.util.concurrent.LinkedBlockingQueue;

// LinkedBlockingQueue的队头队尾都有锁，可以同时put和take，并发程度更高，适合作为缓冲区
// bytesQueue同步阻塞队列作为读磁盘线程和计算线程的缓冲区
// 设计两个bytesQueue的目的是，维持一个byte数组的池子，使byte数组可重复利用
// 无有效数据的byte数组就放在emptyBytesQueue中，有 有效数据的byte数组就放在fullBytesQueue中
// 必须为每对读线程和计算线程配备下面的两个队列，不然无法处理边界问题，因为每个线程领到的任务已经处理好边界了
// 这个Cache不需要很大，但numOfByteArray不能太小，否则多线程会在队列中阻塞
public class ReadAndCalCache {
    public static final int numOfByteArray = 1024;
    public static final int sizeOfByteArray = 1024 * 8;
    public LinkedBlockingQueue<byte[]> emptyBytesQueue;
    public LinkedBlockingQueue<byte[]> fullBytesQueue;

    // 初始化后，emptyBytesQueue中就有了多个可以反复使用的byte数组
    public ReadAndCalCache() {
        emptyBytesQueue = new LinkedBlockingQueue<>();
        fullBytesQueue = new LinkedBlockingQueue<>();
        for (int i = 0; i < numOfByteArray; i++) {
            emptyBytesQueue.add(new byte[sizeOfByteArray]);
        }
    }
}
