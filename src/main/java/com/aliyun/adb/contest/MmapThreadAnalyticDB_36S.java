package com.aliyun.adb.contest;

import com.aliyun.adb.contest.spi.AnalyticDB;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class MmapThreadAnalyticDB_36S implements AnalyticDB {
    private String workspaceDir;
    private final long cacheMemory = 120 * 1024; // KB 所有 load数据 时写文件的缓存之和
//    final int fileNums = 1 << 7;
    private final int fileNums = 1 << 7;
    private CountData countData = new CountData();
    private int[][] preSum;
    private String[] columns;

    Map<String, List<Long>> map = new HashMap<>();
    private int ioThreadNums = 12;
    CountDownLatch countDownLatch;
    private int mmapMaxSize = 40 * 1024; // KB 即40MB
    private int allocateNums;

    class CountData{
        int N = 0;
        int[][] numCounts;
    }

    /**
     *
     * The implementation must contain a public no-argument constructor.
     *
     */
    public MmapThreadAnalyticDB_36S() {
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
//        int a = 1;
//        if(a == 1){
//            throw new  NullPointerException("test IO speed");
//        }

        long queryStart = System.currentTimeMillis();

        int rank = (int) Math.round(countData.N * percentile);
        String fileDir = "";
        int numRank = 0;
        abc:
        for (int i = 0; i < columns.length; i++) {
            if(columns[i].equals(column)){
                for (int index = 0; index < preSum[i].length; index++) {
                    if (preSum[i][index] >= rank){
                        fileDir = this.workspaceDir + File.separator + table +
                                File.separator + column + File.separator + index;
                        numRank = countData.numCounts[i][index] - (preSum[i][index] - rank);
                        break abc;
                    }
                }
            }
        }
//        System.out.println("[" + fileName +"] <> " + "[" + numRank +"]");
        Long ans = 0L;
        if(map.containsKey(fileDir)){
            ans = map.get(fileDir).get(numRank - 1);
            System.out.println("Map work!!!");
        }else {
            ArrayList<Long> nums = new ArrayList<>();
            for(File file: new File(fileDir).listFiles()){
                FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
                int fileSize = (int) fileChannel.size();
                ByteBuffer byteBuffer = ByteBuffer.allocate(fileSize);
                fileChannel.read(byteBuffer);
                byteBuffer.flip();
                for (int i = 0; i < fileSize / 8; i++) {
                    nums.add(byteBuffer.getLong());
                }
            }
            Collections.sort(nums);
            // 查询过的桶，就已经排好序了，因此存在map里，如果下次再查询到这个桶，直接获取即可，但是map存储的数据是
            // 有限的，但是这个地方只需要查询十次，内存还是足够的
            map.put(fileDir, nums);
            ans = nums.get(numRank - 1);
        }

        long queryStop = System.currentTimeMillis();
        System.out.println("query use time: " + (queryStop - queryStart));

        System.out.println("Query:" + table + ", " + column + ", " + percentile + " Answer:" + rank + ", " + ans);
        return ans.toString();
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
        for (String column : columns) {
            for (int i = 0; i < fileNums; i++) {
                File file = new File(workDir, column + File.separator + i);
                if (!file.exists()) {
                    file.mkdirs();
                }
            }
        }

        for (int i = 0; i < ioThreadNums; i++) {
            System.out.println("set current thread: " + i);

            IoTestThread ioTestThread = new IoTestThread(dataFile, readIntervalsList.get(i),
                    mmapMaxSize, workDir, columns, fileNums, allocateNums, countDownLatch, countData);
            Thread thread = new Thread(ioTestThread);
            System.out.println(thread.getName() + " start!");
            thread.start();
        }

        countDownLatch.await();

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
                            CountDownLatch countDownLatch, CountData countData) throws IOException {
            File threadDataFile = new File(dataFile.toString());
            fileChannel = new RandomAccessFile(threadDataFile, "r").getChannel();
            this.readInterval = readInterval;
            this.mmapMaxSize = mmapMaxSize * 1024;  // KB to B
//            this.workDir = workDir;
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

            fileChannels = new FileChannel[columns.length][fileNums];
            byteBuffers = new ByteBuffer[columns.length][fileNums];
            threadNumCounts = new int[columns.length][fileNums];

        }

        @Override
        public void run() {
            int threadDoneNums = 0;
            long mapDoneIndex = this.readInterval[0];
            while (mapDoneIndex + 1 < this.readInterval[1]) {
                int mapSize = 0;
                try {
                    mapSize = getMapSize(fileChannel, mapDoneIndex, mmapMaxSize, this.readInterval[1]);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                MappedByteBuffer mappedByteBuffer = null;
                try {
                    // Linux
                    mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, mapDoneIndex + 1, mapSize);
                    // Windows
//                    mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, mapDoneIndex + 1, mapSize);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                mapDoneIndex += mapSize;
                //            System.out.println("get map size: " + mapSize);

                long[] longTmp = new long[2];
                char[] charTmp = new char[19];
                byte charCount = 0;
                while (mappedByteBuffer.remaining() > 0) {
                    char c = (char) mappedByteBuffer.get();
                    if (c == '\u0000') break; // 文件结束
                    if (c == ',') {
                        longTmp[0] = parseLong(charTmp, charCount);
                        charCount = 0;
                    } else if (c == '\n') {
                        threadDoneNums++;
                        longTmp[1] = parseLong(charTmp, charCount);
//                        //                    System.out.println(Arrays.toString(longTmp));
                        charCount = 0;

                        for (int i = 0; i < longTmp.length; i++) {
                            long num = longTmp[i];

                            int index = (int) (num >>> 56);
                            //                int index = (int)(num >>> 22);
                            if (byteBuffers[i][index] == null) {
                                String fileName = workDir + File.separator + columns[i] +
                                        File.separator + index+ File.separator + Thread.currentThread().getName();
                                try {
                                    fileChannels[i][index] = new RandomAccessFile(new File(fileName), "rw")
                                            .getChannel();
                                } catch (FileNotFoundException e) {
                                    e.printStackTrace();
                                }
                                byteBuffers[i][index] = ByteBuffer.allocate(8 * allocateNums);
                            }

                            byteBuffers[i][index].putLong(num);
                            threadNumCounts[i][index]++;
                            if (byteBuffers[i][index].remaining() == 0) {
                                byteBuffers[i][index].flip();
                                try {
                                    fileChannels[i][index].write(byteBuffers[i][index]);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                                byteBuffers[i][index].clear();
                            }
                        }
                    } else {
                        charTmp[charCount] = c;
                        charCount++;
                    }
                }
            }

            for (int i = 0; i < columns.length; i++) {
                for (int index = 0; index < byteBuffers[0].length; index++) {
                    if (byteBuffers[i][index] != null) {
                        byteBuffers[i][index].flip();
                        try {
                            fileChannels[i][index].write(byteBuffers[i][index]);
                            fileChannels[i][index].close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
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

    public static long parseLong(char[] charTmp, int charCount){
        long res = 0L;
        for (int i = 0; i < charCount; i++) {
            // res *= 10;
            res = (res << 3) + (res << 1);
            res += charTmp[i] - '0';
        }
        return res;
    }

    public static List<long[]> getReadIntervalsList(File dataFile, int ioThreadNums) throws IOException {
        List<long[]> readIntervalsList = new ArrayList<>();
        // Linux
        FileChannel fileChannel = new RandomAccessFile(dataFile, "r").getChannel();
        // Windows
//        FileChannel fileChannel = new RandomAccessFile(dataFile, "rw").getChannel();
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
        MmapThreadAnalyticDB_36S noSortAnalyticDB = new MmapThreadAnalyticDB_36S();
        noSortAnalyticDB.load("/Users/didi/Desktop/test_data", "work");
//        noSortAnalyticDB.load("./test_data", "work");
        long l2 = System.currentTimeMillis();
        System.out.println("load use total: " + (l2 - l));
    }
}
