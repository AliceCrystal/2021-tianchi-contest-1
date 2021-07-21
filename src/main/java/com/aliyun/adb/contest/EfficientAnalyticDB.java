package com.aliyun.adb.contest;

/**
 * @author gt
 * @date 2021/7/3 - 11:24
 */

import com.aliyun.adb.contest.spi.AnalyticDB;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

public class EfficientAnalyticDB implements AnalyticDB {
    public static void main(String[] args) throws Exception {
        EfficientAnalyticDB db = new EfficientAnalyticDB();
        db.load("./test_data", "./work");
    }



    public static final int readerNum = 6;
    public static final int calNum = 6;
    public static final int threadNum = readerNum + calNum;
    public static final int bucketBits = 7;
    public static final int bucketsNum = 1 << bucketBits; // 桶数量，感觉桶数量可以设置多一些，前缀和数组那里可以二分查找
    public static final int rightShift = 63 - bucketBits;

    // 还有一些固定的超参数等用到再设定吧
    public static final int threadBufferSize = 1024 * 1024 * 16; // (16MB)线程中每次MappedByteBuffer内存映射文件的大小
    public static final int longBufferSize = 1024 * 16;
    public static final int quantileBufferSize = 1024 * 8 * 4; // (32K)查询时buffer的大小，因为每一个bucket-x文件平均也就1.5MB


    public static CountDownLatch countDownLatch = new CountDownLatch(threadNum);


    // 类似于即时缓存机制，如果要查询的数据所在的桶已经排好序了
    private Map<Integer, long[]> cacheOrderedBuckets = new HashMap<>();
    public static String tabName;
    public static String[] colName; // 列名
    public static int colLen; // 列数
    public static String workspaceDir; // 存放临时文件的工作目录

    public static int counter = 0; // 记录总共有多少行数据
    public static FileChannel[][][] fileChannels;
    public static ByteBuffer[][][] byteBuffers;
    public static LongAdder[][] dataCounter; // colLen, bucketsNum, 对每一列，每个桶都记录数目
    public static int[][] preSum; // 前缀和数组

    // 使用阻塞队列将任务分发给不同的read thread，默认有1024个任务
    // ArrayBlockingQueue只有一把锁，同时锁住队头和队尾，适合用作任务队列
    public static ArrayBlockingQueue<long[]> taskQueue = new ArrayBlockingQueue<>(1024);
    /*
    多线程版本中，计数器，FileChannel，ByteBuffer都是线程间共享的，需要注意线程安全
     */

    private int times = 0;
    private long queryCost = 0L;
    public static LongAdder readLA = new LongAdder();
    public static LongAdder calLA = new LongAdder();
    /**
     *
     * The implementation must contain a public no-argument constructor.
     *
     */
    public EfficientAnalyticDB() {

    }

    // 计算前缀和，方便使用二分查找
    public void calPreSum(){
        for (int i = 0; i < colLen; i++) {
            for (int j = 0; j < bucketsNum; j++) {
                preSum[i][j + 1] = preSum[i][j] + (int)dataCounter[i][j].sum();
            }
        }
        counter = preSum[0][bucketsNum];
    }

    @Override
    public void load(String tpchDataFileDir, String workspaceDir) throws Exception {
        this.workspaceDir = workspaceDir;

        // tpchDataFileDir是目录名，里面可能有多个表（文本文件）
        File dir = new File(tpchDataFileDir);

        for (File dataFile : dir.listFiles()) {
            System.out.println("Start loading table " + dataFile.getName());

            // You can write data to workspaceDir
            // 工作目录 + 表名
            File yourDataFile = new File(workspaceDir, dataFile.getName());
            // 创建文件夹
            yourDataFile.mkdirs();

            // 将当前表中的数据读入内存中
            loadInMemroy(dataFile);
        }

    }


    // 二分查找左边界，从左右数第一个大于等于target
    private int indexOfBuckets(int rank, int[] preSum){
        int left = 0, right = preSum.length - 1;
        while(left <= right){
            int mid = left + (right - left) / 2;
            if(preSum[mid] < rank){
                left = mid + 1;
            }else if(preSum[mid] >= rank){
                right = mid - 1;
            }
        }
        return left;
    }


    // 读取桶的数据到long数组中，考虑使用多线程来读取，能够加速
    private long[] getBucketData(int indexOfCol, int indexOfBuckets) throws IOException {

        long[] arr = new long[(int)dataCounter[indexOfCol][indexOfBuckets].sum()];

        String path = workspaceDir + File.separator + tabName + File.separator + colName[indexOfCol] +
                File.separator + indexOfBuckets + File.separator + "bucket-";

        int ind = 0;
        for (int i = 0; i < threadNum; i++) {
            FileChannel channel = new RandomAccessFile(new File(path + i), "r").getChannel();
            ByteBuffer buffer = ByteBuffer.allocateDirect(quantileBufferSize);
            while(channel.read(buffer) != -1){
                buffer.flip();
                while(buffer.remaining() != 0)
                    arr[ind++] = buffer.getLong();
                buffer.clear();
            }
        }
        return arr;
    }

    @Override
    public String quantile(String table, String column, double percentile) throws Exception {

        long start = System.currentTimeMillis();
        int rank = (int) Math.round(counter * percentile);

        int indexOfCol = 0;
        for (int i = 0; i < colLen; i++) {
            if(column.equals(colName[i])){
                indexOfCol = i;break;
            }
        }

        // 获取桶的下标
        int indexOfBuckets = indexOfBuckets(rank, preSum[indexOfCol]);
        int relativeRank = rank - preSum[indexOfCol][indexOfBuckets - 1];

        long[] arr;
        int key = indexOfCol * 128 + indexOfBuckets - 1;
        if(cacheOrderedBuckets.containsKey(key)){
            arr = cacheOrderedBuckets.get(key);
        }else{
            // 读取桶的数据到long数组中
            arr = getBucketData(indexOfCol, indexOfBuckets - 1);
            Arrays.sort(arr);
            cacheOrderedBuckets.put(key, arr);
        }

        String ans = String.valueOf(arr[relativeRank - 1]);
        System.out.println("Query:" + table + ", " + column + ", " + percentile + " Answer:" + rank + ", " + ans);

        queryCost += (System.currentTimeMillis() - start);
        times++;
        if(times == 10){
            System.out.println("query cost: " + queryCost);
            //throw new IllegalAccessException("****************************my exception demo*********************************");
        }
        return ans;
    }


    // 通过读取列数据来初始化类中的各类数据
    private void initialize(File dataFile) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(dataFile));
        this.tabName = dataFile.getName();
        this.colName = reader.readLine().split(",");
        this.colLen = colName.length;
        this.dataCounter = new LongAdder[colLen][bucketsNum];
        this.preSum = new int[colLen][bucketsNum + 1]; // 有一个哨兵位置
        // 初始化dataCounter实例
        for (int i = 0; i < colLen; i++) {
            for (int j = 0; j < bucketsNum; j++) {
                dataCounter[i][j] = new LongAdder();
            }
        }
    }

    // 为每一列创建bucket文件夹
    private void createBucketDirs(){
        for(String name: colName){
            for (int i = 0; i < bucketsNum; i++) {
                String path = workspaceDir + File.separator + tabName +  File.separator + name + File.separator + i;
                File file = new File(path);
                file.mkdirs();
            }
        }
    }


    // byteBuffer中写入的是long型数据，因此fileChannels中存的也是long整型数据
    private void initBufAndChannelForEachBucket() throws FileNotFoundException {
        fileChannels = new FileChannel[colLen][bucketsNum][calNum];
        byteBuffers = new ByteBuffer[colLen][bucketsNum][calNum];
        for (int i = 0; i < colLen; i++) {
            for (int j = 0; j < bucketsNum; j++) {
                for (int k = 0; k < calNum; k++) {

                    String relativePath = workspaceDir + File.separator + tabName + File.separator +
                            colName[i] + File.separator + j + File.separator + "bucket-" + k;
                    fileChannels[i][j][k] = new RandomAccessFile(new File(relativePath), "rw").getChannel();
                    byteBuffers[i][j][k] = ByteBuffer.allocateDirect(longBufferSize);
                }
            }
        }
    }


    // 开多个线程从磁盘中读取数据并写入bucket文件中
    private void readAndWrite(File dataFile) throws IOException, InterruptedException {
        FileChannel dataChannel = new RandomAccessFile(dataFile, "r").getChannel();
        Thread[] caler = new Thread[calNum];
        Thread[] reader = new Thread[readerNum];
        for (int i = 0; i < readerNum; i++) {
            ReadAndCalCache[] readAndCalCaches = new ReadAndCalCache[calNum / readerNum];
            for (int j = 0; j < calNum / readerNum; j++) {
                readAndCalCaches[j] = new ReadAndCalCache();
                caler[i * calNum / readerNum + j] = new Thread(new CalThread(readAndCalCaches[j], i * calNum / readerNum + j));
            }
            reader[i] = new Thread(new ReadThread(dataChannel, readAndCalCaches));
        }

        for (Thread t: reader) t.start();
        for (Thread t: caler) t.start();

        countDownLatch.await();
        // 最后需要将writableQueue中剩下的任务完成，并将所有的FileChannel都关闭
        for (int i = 0; i < colLen; i++) {
            for (int j = 0; j < bucketsNum; j++) {
                for (int k = 0; k < calNum; k++) {
                    byteBuffers[i][j][k].flip();
                    fileChannels[i][j][k].write(byteBuffers[i][j][k]);
                    fileChannels[i][j][k].close();
                }
            }
        }

        System.out.println("reader blocking times: " + readLA.sum());
        System.out.println("caler blocking times: " + calLA.sum());
    }

    // 单线程读取数据版本
    private void loadInMemroy(File dataFile) throws IOException, InterruptedException {
        long start = System.currentTimeMillis();

        long init_start = System.currentTimeMillis();
        initialize(dataFile);
        System.out.println("initialize cost: " + (System.currentTimeMillis() - init_start));

        long createBucketDirs_start = System.currentTimeMillis();
        createBucketDirs();
        System.out.println("createBucketDirs cost: " + (System.currentTimeMillis() - createBucketDirs_start));

        long initBufAndChannelForEachBucket_start = System.currentTimeMillis();
        initBufAndChannelForEachBucket();
        System.out.println("initBufAndChannelForEachBucket cost: " + (System.currentTimeMillis() - initBufAndChannelForEachBucket_start));

        long getPieceswise_start = System.currentTimeMillis();
        getPieceswise(dataFile);
        System.out.println("getPieceswise cost: " + (System.currentTimeMillis() - getPieceswise_start));

        long readAndWrite_start = System.currentTimeMillis();
        readAndWrite(dataFile);
        System.out.println("readAndWrite cost: " + (System.currentTimeMillis() - readAndWrite_start));

        System.out.println("load data cost: " + (System.currentTimeMillis() - start));
        // 计算前缀和
        calPreSum();
        System.out.println("data counter: " + preSum[0][bucketsNum]);

    }

    // 分段函数，将整个文件分成piecewiseNum段，这个段数根据每个线程的byteBuffer大小而定
    // 注意这里每一段的字节数都是小于等于threadBufferSize
    // 因此，每个线程的byteBuffer正好可以一次完成这个读取任务
    private void getPieceswise(File dataFile) throws IOException, InterruptedException {
        // 开始读文件
        FileChannel dataChannel = new RandomAccessFile(dataFile, "r").getChannel();
        // 因为dataChannel第一行包括换行符有21个字节
        dataChannel.position(21L);

        // 因为一行最多有40个字节，因此分段的时候要找到'\n'最多可能需要读40个字节
        ByteBuffer buffer = ByteBuffer.allocateDirect(40);
        long startReadPos = 21;
        long endReadPos = 21 + threadBufferSize;
        // 这个表示从dataChannel的endReadPos开始读，并将buffer读满
        while(true){
            dataChannel.read(buffer, endReadPos - 40);
            buffer.flip();
            for (int i = buffer.limit() - 1; i >= 0 ; i--) {
                if(buffer.get(i) == 10){ // 10为换行符的ASCII码
                    // 经过调试，endReadPos最终指向的就是换行符在dataChannel中的position
                    endReadPos = endReadPos - 40 + i;break;
                }
            }
            buffer.clear();
            taskQueue.put(new long[]{startReadPos, endReadPos});
            if(endReadPos == dataChannel.size() - 1) break;
            startReadPos = endReadPos + 1;
            endReadPos = Math.min(startReadPos + threadBufferSize, dataChannel.size());
        }
    }

}

class ReadThread implements Runnable{
    // 给每个read thread一个MappedByteBuffer用于读取dataChannel中的数据
    // read thread 每完成一个任务后就会从任务队列中取新的任务
    private MappedByteBuffer mappedByteBuffer;
    private FileChannel dataChannel;
    private ReadAndCalCache[] readAndCalCaches;
    private int cacheId;


    public ReadThread(FileChannel dataChannel, ReadAndCalCache[] readAndCalCaches){
        this.dataChannel = dataChannel;
        this.readAndCalCaches = readAndCalCaches;
        this.cacheId = 0;
    }

    @Override
    public void run() {
        // 只要有任务线程就不会停
        while(!EfficientAnalyticDB.taskQueue.isEmpty()){
            try {
                // 从任务队列中拿出一个任务
                long[] piece = EfficientAnalyticDB.taskQueue.take();

                mappedByteBuffer = dataChannel.map(FileChannel.MapMode.READ_ONLY, piece[0], piece[1] - piece[0] + 1);
                System.out.println("Start read segment: [" + piece[0] + ", " + piece[1] + "]");

                while(mappedByteBuffer.remaining() >= ReadAndCalCache.sizeOfByteArray) {
                    // 从emptyQueue中取出一个byte数组

                    if (readAndCalCaches[cacheId].emptyBytesQueue.isEmpty()){
                        EfficientAnalyticDB.readLA.add(1L);
                    }
                    // ***************************************************************************************
                    // 这个地方~10%概率会阻塞，说明读线程速度很快，生产byte数组的速度快于计算线程消耗byte数组的速度
                    byte[] bytes = readAndCalCaches[cacheId].emptyBytesQueue.take();

                    // 将数据从磁盘读到byte数组中
                    mappedByteBuffer.get(bytes);

                    // 将byte数组放到fullQueue中

                    readAndCalCaches[cacheId].fullBytesQueue.put(bytes);
                }

                // 但是有一个问题，如果最后mappedByteBuffer剩余的字节数小于bytes.length
                // 调用mappedByteBuffer.get(bytes)会报错，需要做特殊处理
                if(mappedByteBuffer.remaining() > 0){
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
        EfficientAnalyticDB.countDownLatch.countDown();
    }
}

class CalThread implements Runnable{
    private ReadAndCalCache readAndCalCache;
    private int threadId;
    public CalThread(ReadAndCalCache readAndCalCache, int threadId){
        this.readAndCalCache = readAndCalCache;
        this.threadId = threadId;
    }

    // 那么问题来了，Calculate Thread什么时候结束呢? 读到文件结束符就结束
    @Override
    public void run() {
        long[] nums = new long[EfficientAnalyticDB.colLen];
        long tmp = 0;
        outer:
        while(true){
            try {
                // 从fullQueue中取出一个字节数组，开始计算
                if(readAndCalCache.fullBytesQueue.isEmpty()){
                    EfficientAnalyticDB.calLA.add(1L);
                }
                // 大约0.2%概率该队列中的byte数组为空，说明读线程生产byte数组速度足够快
                byte[] bytes = readAndCalCache.fullBytesQueue.take();
                for(byte b: bytes){
                    if (b >= 48) {
                        tmp = (tmp << 3) + (tmp << 1); // 相当于乘以10
                        tmp += b - 48; // '0'的ASCII代码就是48
                    } else if (b == 44) {
                        // 44 为逗号的ASCII码
                        // 读到第一个数，开始读第二个数
                        nums[0] = tmp;
                        tmp = 0;
                    } else if (b == 10){
                        // 10 为 '\n'的ASCII码
                        nums[1] = tmp;
                        tmp = 0;
//                        for (int i = 0; i < nums.length; i++) {
//                            int index = (int) (nums[i] >>> EfficientAnalyticDB.rightShift);
//
//                            EfficientAnalyticDB.byteBuffers[i][index][threadId].putLong(nums[i]);
//
//                            EfficientAnalyticDB.dataCounter[i][index].add(1L);
//
//                            if(EfficientAnalyticDB.byteBuffers[i][index][threadId].remaining() == 0){
//                                EfficientAnalyticDB.byteBuffers[i][index][threadId].flip();
//                                EfficientAnalyticDB.fileChannels[i][index][threadId].write(EfficientAnalyticDB.byteBuffers[i][index][threadId]);
//                                EfficientAnalyticDB.byteBuffers[i][index][threadId].clear();
//                            }
//                        }

                    } else {
                        // 即结束符，这里的结束符是我为每一对读线程和计算线程设计的
                        break outer;
                    }
                }
                // 如果是半包就不再放回emptyQueue，因为它无法复用
                if(bytes.length == ReadAndCalCache.sizeOfByteArray){
                    readAndCalCache.emptyBytesQueue.put(bytes);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        EfficientAnalyticDB.countDownLatch.countDown();
    }

}

// LinkedBlockingQueue的队头队尾都有锁，可以同时put和take，并发程度更高，适合作为缓冲区
// bytesQueue同步阻塞队列作为读磁盘线程和计算线程的缓冲区
// 设计两个bytesQueue的目的是，维持一个byte数组的池子，使byte数组可重复利用
// 无有效数据的byte数组就放在emptyBytesQueue中，有 有效数据的byte数组就放在fullBytesQueue中
// 必须为每对读线程和计算线程配备下面的两个队列，不然无法处理边界问题，因为每个线程领到的任务已经处理好边界了
// 这个Cache不需要很大，但numOfByteArray不能太小，否则多线程会在队列中阻塞
class ReadAndCalCache{
    public static final int numOfByteArray = 1024;
    public static final int sizeOfByteArray = 1024 * 8;
    public LinkedBlockingQueue<byte[]> emptyBytesQueue;
    public LinkedBlockingQueue<byte[]> fullBytesQueue;

    // 初始化后，emptyBytesQueue中就有了多个可以反复使用的byte数组
    public ReadAndCalCache(){
        emptyBytesQueue = new LinkedBlockingQueue<>();
        fullBytesQueue = new LinkedBlockingQueue<>();
        for (int i = 0; i < numOfByteArray; i++) {
            emptyBytesQueue.add(new byte[sizeOfByteArray]);
        }
    }
}

