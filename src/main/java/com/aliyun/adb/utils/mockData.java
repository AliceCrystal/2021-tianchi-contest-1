package com.aliyun.adb.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class mockData {
    public static void main(String[] args) throws IOException {
        String from = "./test_data/lineitem";
        String to = "./test_data/lineitemss";
        File file = new File(to);

        FileChannel channel = new RandomAccessFile(new File(from), "r").getChannel();
        ByteBuffer colBuffer = ByteBuffer.allocate(21);
        ByteBuffer byteBuffer = ByteBuffer.allocate((int)channel.size() - 21);
        channel.read(colBuffer);
        colBuffer.flip();

        channel.read(byteBuffer);
        byteBuffer.flip();

        FileChannel inChannel = new RandomAccessFile(file, "rw").getChannel();
        inChannel.write(colBuffer);
        for (int i = 0; i < 10000; i++) {
            if(i % 1000 == 0){
                System.out.println(i);
            }
            inChannel.write(byteBuffer);
            byteBuffer.rewind();
        }

        ByteBuffer enterBuffer = ByteBuffer.allocate(1);
        enterBuffer.put((byte)('\n'));
        inChannel.write(enterBuffer);

        inChannel.close();
        channel.close();
//        FileChannel channel = new RandomAccessFile(new File("./test_data/lineitemss"), "r").getChannel();
//        System.out.println(channel.size());
//        ByteBuffer byteBuffer = ByteBuffer.allocate((int)channel.size());
//        channel.read(byteBuffer);
//        byteBuffer.flip();
//        System.out.println(byteBuffer.get(0)); // 读第一行第一个byte
//        System.out.println(byteBuffer.get(20)); // 读第一行最后一个byte，也就是换行符
//        System.out.println(byteBuffer.get((int)channel.size() - 1)); // 读文件的最后一个字符，居然仍然是换行符，可能windows没有文件结尾符号
    }
}
