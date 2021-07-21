package com.aliyun.adb.contest.inner;

/**
 * @author gt
 * @date 2021/7/3 - 11:24
 */

import com.aliyun.adb.contest.cache.CalAndWriteCache;
import com.aliyun.adb.contest.cache.ReadAndCalCache;
import com.aliyun.adb.contest.spi.AnalyticDB;
import com.aliyun.adb.contest.thread.CalThread;
import com.aliyun.adb.contest.thread.ReadThread;
import com.aliyun.adb.contest.thread.WriteThread;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

public class ReaderAndCalerAndWriter implements AnalyticDB {
    public static void main(String[] args) throws Exception {
        ReaderAndCalerAndWriter db = new ReaderAndCalerAndWriter();
        db.load("/Users/didi/Desktop/test_data_2", "./work");
    }

    public static final int readerNum = 4;
    public static final int calNum = 4;
    public static final int writeNum = 4;
    public static final int threadNum = readerNum + calNum + writeNum;
    public static final int bucketBits = 7;
    public static final int bucketsNum = 1 << bucketBits; // 桶数量，感觉桶数量可以设置多一些，前缀和数组那里可以二分查找
    public static final int rightShift = 63 - bucketBits;

    // 还有一些固定的超参数等用到再设定吧
    public static final int threadBufferSize = 1024 * 16; // (16MB)线程中每次MappedByteBuffer内存映射文件的大小
    public static final int longBufferSize = 1024 * 16;
    public static final int quantileBufferSize = 8 * 4; // (32K)查询时buffer的大小，因为每一个bucket-x文件平均也就1.5MB


    public static CountDownLatch countDownLatch = new CountDownLatch(threadNum);


    // 类似于即时缓存机制，如果要查询的数据所在的桶已经排好序了
    private Map<Integer, long[]> cacheOrderedBuckets = new HashMap<>();
    public static String tabName;
    public static String[] colName; // 列名
    public static int colLen; // 列数
    public static String workspaceDir; // 存放临时文件的工作目录

    public static int counter = 0; // 记录总共有多少行数据
    public static FileChannel[][][] fileChannels;
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
    public ReaderAndCalerAndWriter() {

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
    private void initChannelForEachBucket() throws FileNotFoundException {
        fileChannels = new FileChannel[colLen][bucketsNum][writeNum];
        for (int i = 0; i < colLen; i++) {
            for (int j = 0; j < bucketsNum; j++) {
                for (int k = 0; k < writeNum; k++) {
                    String relativePath = workspaceDir + File.separator + tabName + File.separator +
                            colName[i] + File.separator + j + File.separator + "bucket-" + k;
                    fileChannels[i][j][k] = new RandomAccessFile(new File(relativePath), "rw").getChannel();
                }
            }
        }
    }


    // 开多个线程从磁盘中读取数据并写入bucket文件中
    // 之前这么写的原因是因为读数据的线程速度很快，为了匹配速度，我们让一个读线程拥有多个readAndCalCache
    // 每个计算线程仍然只对应一个readAndCalCache，但是最新发现，读速度，计算速度，写速度在不同的硬件下是不一样的，调参需要在阿里云上做
    private void readAndWrite(File dataFile) throws IOException, InterruptedException {
        FileChannel dataChannel = new RandomAccessFile(dataFile, "r").getChannel();
        Thread[] caler = new Thread[calNum];
        Thread[] reader = new Thread[readerNum];
        Thread[] writer = new Thread[writeNum];

        CalAndWriteCache calAndWriteCache = new CalAndWriteCache();

        for (int i = 0; i < readerNum; i++) {
            ReadAndCalCache[] readAndCalCaches = new ReadAndCalCache[calNum / readerNum];
            for (int j = 0; j < calNum / readerNum; j++) {
                readAndCalCaches[j] = new ReadAndCalCache();
                caler[i * calNum / readerNum + j] = new Thread(new CalThread(i * calNum / readerNum + j, readAndCalCaches[j], calAndWriteCache));
            }
            reader[i] = new Thread(new ReadThread(dataChannel, readAndCalCaches));
        }
        for (int i = 0; i < writeNum; i++) {
            writer[i] = new Thread(new WriteThread(i, calAndWriteCache, fileChannels));
        }

        for (Thread r: reader) r.start();
        for (Thread t: caler) t.start();

        for (Thread w: writer) w.start();

        countDownLatch.await();
        // 将所有的FileChannel都关闭
        for (int i = 0; i < colLen; i++) {
            for (int j = 0; j < bucketsNum; j++) {
                for (int k = 0; k < calNum; k++) {
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
        initChannelForEachBucket();
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


