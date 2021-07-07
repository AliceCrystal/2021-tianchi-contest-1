package com.aliyun.adb.contest;

import com.aliyun.adb.contest.spi.AnalyticDB;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class MmapThreadAnalyticDB implements AnalyticDB {
    private String workspaceDir;
    private final long cacheMemory = 120 * 1024; // KB 所有 load数据 时写文件的缓存之和
    private final int fileControl = 8;
    private final int bucketControl = 11;
    private final int fileNums = 1 << fileControl;
    private CountData countData = new CountData();
    private int[][] preSum;
    private String[] columns;

    private int ioThreadNums = 12;
    private int queryThreadNums = 8;
    CountDownLatch countDownLatch;
    private int mmapMaxSize = 40 * 1024; // KB
    private int allocateNums;
    long totalquerytime = 0;
    FileChannel[][] fileChannels;
    class CountData{
        int N = 0;
        int[][] numCounts;
    }

    int quantileTimes = 0;

    /**
     *
     * The implementation must contain a public no-argument constructor.
     *
     */
    public MmapThreadAnalyticDB() {
    }

    @Override
    public void load(String tpchDataFileDir, String workspaceDir) throws Exception {
        long loadStart = System.currentTimeMillis();

        this.workspaceDir = workspaceDir;
        File dir = new File(tpchDataFileDir);
        for (File dataFile : dir.listFiles()) {
            System.out.println("Start loading table " + dataFile.getName());

            // You can write data to workspaceDir
            File yourDataFile = new File(workspaceDir, dataFile.getName());  // 工作目录 + 表名
            if(!yourDataFile.exists())
                yourDataFile.mkdirs();

            loadInMemory(dataFile, yourDataFile);
        }

        long loadStop = System.currentTimeMillis();
        System.out.println("load use time: " + (loadStop - loadStart));
    }

    @Override
    public String quantile(String table, String column, double percentile) throws Exception {
        CountDownLatch countDownLatch;
        if(++quantileTimes == 10){
            // 用来抛异常测试
            System.out.println("total query time: " + totalquerytime);
            throw new  NullPointerException("test IO speed");
        }

        long queryStart = System.currentTimeMillis();

        int bucketNums = 1 << bucketControl;
        Object[] lock = new Object[bucketNums];
        for (int i = 0; i < lock.length; i++) {
            lock[i] = new Object();
        }
        List<Long>[] bucketLists = new List[bucketNums];
        for (int i = 0; i < bucketLists.length; i++) {
            bucketLists[i] = new ArrayList<>();
        }
        int rank = (int) Math.round(countData.N * percentile);
        String fileName = "";
        int numRank = 0;
        abc:
        for (int i = 0; i < columns.length; i++) {
            if(columns[i].equals(column)){
                for (int index = 0; index < preSum[i].length; index++) {
                    if (preSum[i][index] >= rank){
                        fileName = this.workspaceDir + File.separator + table +
                                File.separator + column + File.separator + index;
                        numRank = countData.numCounts[i][index] - (preSum[i][index] - rank);
                        break abc;
                    }
                }
            }
        }
        long fileSize = new RandomAccessFile(new File(fileName), "r").getChannel().size();
        long numNums = fileSize / 8;
        long numPerThread = numNums / queryThreadNums;
        countDownLatch = new CountDownLatch(queryThreadNums);
        for(int i = 0; i < queryThreadNums; ++i){
            long numStartIndex = i * numPerThread;
            if(i == queryThreadNums - 1){
                numPerThread = numNums - (queryThreadNums - 1) * numPerThread;
            }
            Thread thread = new Thread(new ReadFile(new File(fileName),
                    numStartIndex, numPerThread, bucketLists, lock, countDownLatch));
            thread.start();
        }
        countDownLatch.await();
        Long ans = 0L;
        for (int i = 0; i < bucketLists.length; i++) {
            if (numRank > bucketLists[i].size()) {
                numRank -= bucketLists[i].size();
            }else {
                Collections.sort(bucketLists[i]);
                ans = bucketLists[i].get(numRank - 1);
                break;
            }
        }

        long queryStop = System.currentTimeMillis();
        System.out.println("single query use time: " + (queryStop - queryStart));

        System.out.println("Query:" + table + ", " + column + ", " + percentile + " Answer:" + rank + ", " + ans);
        totalquerytime += queryStop - queryStart;
        return ans.toString();
    }

    class ReadFile implements Runnable{
        List<Long>[] bucket;
        Object[] lock;
        MappedByteBuffer mappedByteBuffer;
        CountDownLatch countDownLatch;
        public ReadFile(File file, long numStartIndex,
                        long numPerThread, List<Long>[] bucket, Object[] lock,
                        CountDownLatch countDownLatch) throws IOException {
            this.bucket = bucket;
            this.lock = lock;
            this.countDownLatch = countDownLatch;
            mappedByteBuffer = new RandomAccessFile(file, "r").getChannel()
                    .map(FileChannel.MapMode.READ_ONLY, numStartIndex * 8, numPerThread * 8);
        }
        @Override
        public void run() {
            Long mask = Long.MAX_VALUE;
            mask <<= (fileControl + 1);
            mask >>>= (64 - bucketControl);
            mask <<= (64 - bucketControl - fileControl - 1);
            int rightShift = 64 - fileControl - bucketControl - 1;
            while(mappedByteBuffer.remaining() > 0){
                long num = mappedByteBuffer.getLong();
                int idx = (int)((num & mask) >> rightShift);
                synchronized (lock[idx]){
                    bucket[idx].add(num);
                }
            }
            countDownLatch.countDown();
        }
    }

    private void loadInMemory(File dataFile, File workDir) throws IOException, InterruptedException {
        List<long[]> readIntervalsList = getReadIntervalsList(dataFile, ioThreadNums);

        // bug 补丁
        ioThreadNums = readIntervalsList.size();
        countDownLatch = new CountDownLatch(ioThreadNums);
        allocateNums = (int) (cacheMemory * 1024 / (8 * fileNums * ioThreadNums));

        BufferedReader reader = new BufferedReader(new FileReader(dataFile));
        columns = reader.readLine().split(",");

        countData.numCounts = new int[columns.length][fileNums];
        fileChannels = new FileChannel[columns.length][fileNums];
        for (int i=0; i < columns.length; i++) {
            new File(workDir, columns[i]).mkdir();
            for (int index = 0; index < fileNums; index++) {
                fileChannels[i][index] = new RandomAccessFile(
                        new File(workDir, columns[i] + File.separator + index), "rw").getChannel();
            }
        }

        for (int i = 0; i < ioThreadNums; i++) {
            System.out.println("set current thread: " + i);

            IoTestThread ioTestThread = new IoTestThread(dataFile, readIntervalsList.get(i),
                    mmapMaxSize, workDir, columns, fileNums, allocateNums, countDownLatch, countData, fileChannels);
            Thread thread = new Thread(ioTestThread);
            System.out.println(thread.getName() + " start!");
            thread.start();
        }

        countDownLatch.await();
        for (int i = 0; i < columns.length; i++) {
            for (int index = 0; index < fileNums; index++) {
                fileChannels[i][index].close();
            }
        }

        preSum = new int[countData.numCounts.length][countData.numCounts[0].length];
        for (int i = 0; i < preSum.length; i++) {
            preSum[i][0] = countData.numCounts[i][0];
            for (int index = 1; index < preSum[0].length; index++) {
                preSum[i][index] = preSum[i][index-1] + countData.numCounts[i][index];
            }
        }
        for (int i = 0; i < preSum.length; i++) {
            System.out.println("column: " + i + " tatal nums: " + preSum[i][preSum[0].length - 1]);
        }
        System.out.println("load done.");
    }

    public int getMapSize(FileChannel fileChannel, long mapDoneIndex, int maxSize, long endReadIndex) throws IOException {
        if(mapDoneIndex + maxSize >= endReadIndex){
            return (int) (endReadIndex - mapDoneIndex);
        }
        int size = maxSize;
        ByteBuffer byteBuffer = ByteBuffer.allocate(1);
        fileChannel.read(byteBuffer, mapDoneIndex + size);
        byteBuffer.flip();
        while ((char)byteBuffer.get() != '\n'){
            size--;
            byteBuffer.clear();
            fileChannel.read(byteBuffer, mapDoneIndex + size);
            byteBuffer.flip();
        }
        return size;
    }

    class IoTestThread implements Runnable {
        String[] columns;
        int fileNums;
        int allocateNums;
        FileChannel[][] fileChannels;
//        Object[][] fileChannelLock;
        ByteBuffer[][] byteBuffers;
        int[][] threadNumCounts;
        long[] readInterval;
        int mmapMaxSize;
        private FileChannel fileChannel;
        File workDir;
        CountDownLatch countDownLatch;
        CountData countData;


        public IoTestThread(File dataFile, long[] readInterval, int mmapMaxSize, File workDir,
                            String[] columns, int fileNums, int allocateNums,
                            CountDownLatch countDownLatch, CountData countData,
                            FileChannel[][] fileChannels) throws IOException {
            File threadDataFile = new File(dataFile.toString());
            fileChannel = new RandomAccessFile(threadDataFile, "r").getChannel();
            this.readInterval = readInterval;
            this.mmapMaxSize = mmapMaxSize * 1024;  // KB to B
            this.workDir = new File(workDir.toString());
            this.columns = new String[columns.length];
            for (int i = 0; i < columns.length; i++) {
                this.columns[i] = columns[i];
            }
            this.columns = columns;
            this.fileNums = fileNums;
            this.allocateNums = allocateNums;
            this.countDownLatch = countDownLatch;
            this.countData = countData;

            // 传入
            this.fileChannels = fileChannels;


            byteBuffers = new ByteBuffer[columns.length][fileNums];
            threadNumCounts = new int[columns.length][fileNums];
        }

        @Override
        public void run() {
            for (int i = 0; i < columns.length; i++) {
                for (int index = 0; index < byteBuffers[0].length; index++) {
                    byteBuffers[i][index] = ByteBuffer.allocate(8 * allocateNums);
                }
            }

            int threadDoneNums = 0;
            long mapDoneIndex = this.readInterval[0];
            long longNumber;
            int rightShift = 63 - fileControl;
            int columnIndex;
            while (mapDoneIndex + 1 < this.readInterval[1]) {
                int mapSize = 0;
                try {
                    mapSize = getMapSize(fileChannel, mapDoneIndex, mmapMaxSize, this.readInterval[1]);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                MappedByteBuffer mappedByteBuffer = null;
                try {
                    mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, mapDoneIndex + 1, mapSize);

                } catch (IOException e) {
                    e.printStackTrace();
                }
                mapDoneIndex += mapSize;
                longNumber = 0;

                while (mappedByteBuffer.remaining() > 0) {
                    char c = (char) mappedByteBuffer.get();
                    if(c >= '0') {
                        longNumber = (longNumber << 3) + (longNumber << 1);
                        longNumber += c - '0';
                    }else{
                        if(c == ','){
                            columnIndex = 0;
                        }else {
                            columnIndex = 1;
                            threadDoneNums++;
                        }
                        // 存数据
                        int index = (int) (longNumber >>> rightShift);
                        byteBuffers[columnIndex][index].putLong(longNumber);
                        threadNumCounts[columnIndex][index]++;
                        if (byteBuffers[columnIndex][index].remaining() == 0) {
                            byteBuffers[columnIndex][index].flip();
                            try {
                                fileChannels[columnIndex][index].write(byteBuffers[columnIndex][index]);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            byteBuffers[columnIndex][index].clear();
                        }

                        // 重置
                        longNumber = 0;
                    }
                }
            }

            for (int i = 0; i < columns.length; i++) {
                for (int index = 0; index < byteBuffers[0].length; index++) {
                    byteBuffers[i][index].flip();
                    try {
                        fileChannels[i][index].write(byteBuffers[i][index]);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            synchronized (countData){
                for (int i = 0; i < threadNumCounts.length; i++) {
                    for (int index = 0; index < threadNumCounts[0].length; index++) {
                        countData.numCounts[i][index] += threadNumCounts[i][index];
                    }
                }
                countData.N += threadDoneNums;
            }

            System.out.println(Thread.currentThread().getName() + " have done: " + threadDoneNums);
            System.out.println(Thread.currentThread().getName() + " finish!");
            countDownLatch.countDown();
        }
    }

    public static List<long[]> getReadIntervalsList(File dataFile, int ioThreadNums) throws IOException {
        List<long[]> readIntervalsList = new ArrayList<>();
        FileChannel fileChannel = new RandomAccessFile(dataFile, "r").getChannel();
        int byteBufferSize = 40;
        ByteBuffer byteBuffer = ByteBuffer.allocate(byteBufferSize);
        long singleFileReadChars = (fileChannel.size() - 21 + ioThreadNums - 1) / ioThreadNums;

        long fileChannelReadStartIndex = 20;
        for (int i = 0; i < ioThreadNums; i++) {
            if(i == ioThreadNums - 1){
                long[] readInterval = new long[]{fileChannelReadStartIndex, fileChannel.size() - 1};
                readIntervalsList.add(readInterval);
                break;
            }
            long fileChannelReadEndIndex = singleFileReadChars + fileChannelReadStartIndex;

            fileChannel.read(byteBuffer, fileChannelReadEndIndex);
            byteBuffer.flip();

            for (int lineBreakIndex = 0; lineBreakIndex < byteBufferSize; lineBreakIndex++) {
                if (byteBuffer.get(lineBreakIndex) == (int) '\n') {
                    long[] readInterval = new long[]{fileChannelReadStartIndex, fileChannelReadEndIndex + lineBreakIndex};
                    readIntervalsList.add(readInterval);
                    fileChannelReadStartIndex = fileChannelReadEndIndex + lineBreakIndex;
                    break;
                }
            }
            byteBuffer.clear();

        }
        return readIntervalsList;
    }


    public static void main(String[] args) throws Exception {
        long l = System.currentTimeMillis();
        MmapThreadAnalyticDB noSortAnalyticDB = new MmapThreadAnalyticDB();
        noSortAnalyticDB.load("./ceshi-300000000", "work");
//        noSortAnalyticDB.load("./test_data", "work");
        long l2 = System.currentTimeMillis();
        System.out.println("load use total: " + (l2 - l));
    }
}