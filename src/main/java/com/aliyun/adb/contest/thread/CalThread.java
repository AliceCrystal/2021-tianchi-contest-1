package com.aliyun.adb.contest.thread;

import com.aliyun.adb.contest.cache.ByteBufferCache;
import com.aliyun.adb.contest.cache.CalAndWriteCache;
import com.aliyun.adb.contest.cache.ReadAndCalCache;
import com.aliyun.adb.contest.core.ReaderAndCalerAndWriter;

public class CalThread implements Runnable {
    public int threadId;
    public ReadAndCalCache readAndCalCache;
    public ByteBufferCache[][] byteBufferCaches;
    public CalAndWriteCache calAndWriteCache;

    public CalThread(int threadId, ReadAndCalCache readAndCalCache, CalAndWriteCache calAndWriteCache,
                     ByteBufferCache[][] byteBufferCaches) {
        this.threadId = threadId;
        this.readAndCalCache = readAndCalCache;
        this.byteBufferCaches = byteBufferCaches;
        this.calAndWriteCache = calAndWriteCache;
    }

    // 那么问题来了，Calculate Thread什么时候结束呢? 读到文件结束符就结束
    @Override
    public void run() {
        long[] nums = new long[ReaderAndCalerAndWriter.colLen];
        long tmp = 0;
        outer:
        while (true) {
            try {
                // 从fullQueue中取出一个字节数组，开始计算
                if (readAndCalCache.fullBytesQueue.isEmpty()) {
                    ReaderAndCalerAndWriter.calLA.add(1L);
                }
                byte[] bytes = readAndCalCache.fullBytesQueue.take();

                for (byte b : bytes) {
                    if (b >= 48) {
                        tmp = (tmp << 3) + (tmp << 1); // 相当于乘以10
                        tmp += b - 48; // '0'的ASCII代码就是48
                    } else if (b == 44) {
                        // 44 为逗号的ASCII码
                        // 读到第一个数，开始读第二个数
                        nums[0] = tmp;
                        tmp = 0;
                    } else if (b == 10) {
                        // 10 为 '\n'的ASCII码
                        nums[1] = tmp;
                        tmp = 0;
                        for (int i = 0; i < nums.length; i++) {
                            int index = (int) (nums[i] >>> ReaderAndCalerAndWriter.rightShift);

                            if(byteBufferCaches[i][index].currentBuffer == null){
                                byteBufferCaches[i][index].updateCurrentByteBuffer();
                            }
                            byteBufferCaches[i][index].currentBuffer.putLong(nums[i]);

                            ReaderAndCalerAndWriter.dataCounter[i][index].add(1L);

                            // ByteBufferCache中的currentBuffer写满了，便可以将数据放进CalAndWriteCache中了
                            if(byteBufferCaches[i][index].currentBuffer.remaining() == 0){
                                byteBufferCaches[i][index].toFullBufferQueue();
                                calAndWriteCache.put(byteBufferCaches[i][index]);
                            }
                            // 至于对写满的byteBufffer的处理就由写线程来完成了
                        }

                    } else {
                        // 即结束符，这里的结束符是我为每一对读线程和计算线程设计的
                        // 这时候ByteBufferCache中的currentBuffer可能还有未写满的数据
                        for (int i = 0; i < ReaderAndCalerAndWriter.colLen; i++) {
                            for (int j = 0; j < ReaderAndCalerAndWriter.bucketsNum; j++) {
                                if (byteBufferCaches[i][j].currentBuffer != null){
                                    byteBufferCaches[i][j].toFullBufferQueue();
                                    calAndWriteCache.put(byteBufferCaches[i][j]);
                                }
                            }
                        }
                        calAndWriteCache.put(new ByteBufferCache(-1, -1));
                        break outer;
                    }
                }
                // 如果是半包就不再放回emptyQueue，因为它无法复用
                if (bytes.length == ReadAndCalCache.sizeOfByteArray) {
                    readAndCalCache.emptyBytesQueue.put(bytes);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("caler-" + threadId + " done.");
        ReaderAndCalerAndWriter.countDownLatch.countDown();
    }

}
