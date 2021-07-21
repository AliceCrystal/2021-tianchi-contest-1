package com.aliyun.adb.contest;

import com.aliyun.adb.contest.spi.AnalyticDB;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.LongAdder;

public class SimpleAnalyticDB implements AnalyticDB {
    public static void main(String[] args) throws Exception {
//        SimpleAnalyticDB db = new SimpleAnalyticDB();
//        db.load("./test_data", "./work");
    }

    public static final int threadNum = 8;
    public static final int bucketBits = 8;
    public static final int bucketsNum = 1 << bucketBits; // 桶数量，感觉桶数量可以设置多一些，前缀和数组那里可以二分查找
    public static final int rightShift = 63 - bucketBits;
    // 还有一些固定的超参数等用到再设定吧

//    public static final int threadBufferSize = 1024 * 1024 * 16; // (16MB)线程中buffer的大小
//    public static final int longBufferSize = 1024 * 8 * 4; //(32KB)
//    public static final int quantileBufferSize = 1024 * 8 * 4; // (32K)查询时buffer的大小，因为每一个bucket-x文件平均也就1.5MB

    public static final int threadBufferSize = 1024; // (16MB)线程中buffer的大小
    public static final int longBufferSize = 128; //(128K)
    public static final int quantileBufferSize = 64; // (64K)查询时buffer的大小，因为每一个bucket-x文件平均也就1.5MB

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

    // 使用阻塞队列将任务分发给不同的worker，默认有1024个任务
    public static ArrayBlockingQueue<long[]> piecewises = new ArrayBlockingQueue<>(1024);



    /*
    多线程版本中，计数器，FileChannel，ByteBuffer都是线程间共享的，需要注意线程安全
     */

    private int times = 0;
    private long queryCost = 0L;
    public static LongAdder la = new LongAdder();
    /**
     *
     * The implementation must contain a public no-argument constructor.
     *
     */
    public SimpleAnalyticDB() {

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

    // 每一个桶都对应一个fileChanel和byteBuffer
    // byteBuffer中写入的是long型数据，因此fileChannels中存的也是long整型数据
    private void initBufAndChannelForEachBucket() throws FileNotFoundException {
        fileChannels = new FileChannel[colLen][bucketsNum][threadNum];
        byteBuffers = new ByteBuffer[colLen][bucketsNum][threadNum];
        // 在外面直接初始化对应的channel 和 buffer
        for (int i = 0; i < colLen; i++) {
            for (int j = 0; j < bucketsNum; j++) {
                for (int k = 0; k < threadNum; k++) {
                    byteBuffers[i][j][k] = ByteBuffer.allocateDirect(longBufferSize);
                    String relativePath = workspaceDir + File.separator + tabName + File.separator +
                            colName[i] + File.separator + j + File.separator + "bucket-" + k;
                    fileChannels[i][j][k] = new RandomAccessFile(new File(relativePath), "rw").getChannel();
                }
            }
        }
    }


    // 开多个线程从磁盘中读取数据并写入bucket文件中
    private void readAndWrite(File dataFile) throws IOException, InterruptedException {
        FileChannel dataChannel = new RandomAccessFile(dataFile, "r").getChannel();
        Thread[] workers = new Thread[threadNum];
        for (int i = 0; i < threadNum; i++) {
            workers[i] = new Thread(new Worker(dataChannel, i));
            workers[i].start();
        }
        countDownLatch.await();
        // 这里还需要对最后没有写满的ByteBuffer打补丁
        for (int i = 0; i < colLen; i++) {
            for (int j = 0; j < bucketsNum; j++) {
                for (int k = 0; k < threadNum; k++) {
                    byteBuffers[i][j][k].flip();
                    fileChannels[i][j][k].write(byteBuffers[i][j][k]);
                    fileChannels[i][j][k].close();
                }
            }
        }
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


        System.out.println("get much bytes cost: " + la.sum());
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
            piecewises.put(new long[]{startReadPos, endReadPos});
            if(endReadPos == dataChannel.size() - 1) break;
            startReadPos = endReadPos + 1;
            endReadPos = Math.min(startReadPos + threadBufferSize, dataChannel.size());
        }
    }

}

class Worker implements Runnable{
    // 给每个worker一个buffer用于读取dataChannel中的数据
    // work完成一个任务后就会向主线程申请新的任务
    private MappedByteBuffer mappedByteBuffer;
    private FileChannel dataChannel;
    private int threadId;



    public Worker(FileChannel dataChannel, int threadId){
        this.dataChannel = dataChannel;
        this.threadId = threadId;
    }

    @Override
    public void run() {
        // 只要有任务线程就不会停
        while(!SimpleAnalyticDB.piecewises.isEmpty()){
            try {
                // 从同步队列中拿出一个任务，这个应该不会占用很长时间吧
                long[] piece = SimpleAnalyticDB.piecewises.take();
                // 一级缓存，目前是16M
                // 二级缓存，目前是channel对应的byteBuffer，只有64KB
                // 因此考虑使用三级缓存
                mappedByteBuffer = dataChannel.map(FileChannel.MapMode.READ_ONLY, piece[0], piece[1] - piece[0] + 1);

                long first = 0, second = 0;
//                int col = 0;
                long tmp = 0;
                while(mappedByteBuffer.remaining() != 0){
                    byte b = mappedByteBuffer.get();
                    switch(b){
                        case 44 : // 44 为逗号的ASCII码
                            // 读到第一个数，开始读第二个数
                            first = tmp;tmp=0;
                            break;
                        case 10: // 10 为换行符的ASCII码
                            // 读到第二个数,并且读入对应的buffer中
                            second = tmp;tmp=0;
                            // 对一行数据做读写处理
                            int index = (int) (first >>> SimpleAnalyticDB.rightShift);
                            SimpleAnalyticDB.byteBuffers[0][index][threadId].putLong(first);
                            SimpleAnalyticDB.dataCounter[0][index].add(1L);

                            if(SimpleAnalyticDB.byteBuffers[0][index][threadId].remaining() == 0){
                                SimpleAnalyticDB.byteBuffers[0][index][threadId].flip();
                                SimpleAnalyticDB.fileChannels[0][index][threadId].write(SimpleAnalyticDB.byteBuffers[0][index][threadId]);
                                SimpleAnalyticDB.byteBuffers[0][index][threadId].clear();
                            }

                            index = (int) (second >>> SimpleAnalyticDB.rightShift);
                            SimpleAnalyticDB.byteBuffers[1][index][threadId].putLong(second);
                            SimpleAnalyticDB.dataCounter[1][index].add(1L);

                            // byteBuffers写满了，可以往对应的chanel中写了，据说Filechannel本身就是线程安全的
                            // 每写入一行数据就判断一次byteBuffer满了吗？是否很浪费计算资源

                            if(SimpleAnalyticDB.byteBuffers[1][index][threadId].remaining() == 0){
                                SimpleAnalyticDB.byteBuffers[1][index][threadId].flip();
                                SimpleAnalyticDB.fileChannels[1][index][threadId].write(SimpleAnalyticDB.byteBuffers[1][index][threadId]);
                                SimpleAnalyticDB.byteBuffers[1][index][threadId].clear();
                            }

                            break;
                        default:
                            tmp = (tmp << 3) + (tmp << 1);
                            tmp += b - 48;
//                            tmpNums[col] = (tmpNums[col] << 3) + (tmpNums[col] << 1); // 相当于乘以10
//                            tmpNums[col] += b - 48; // '0'的ASCII代码就是48
                    }
                }

            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            }
        }
        SimpleAnalyticDB.countDownLatch.countDown();
        System.out.println("Thread-" + threadId + " done!");
    }
}