package com.splicemachine.encoding;

import com.splicemachine.derby.stats.Accumulator;
import com.splicemachine.derby.stats.Stats;
import com.splicemachine.derby.stats.TimingStats;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Ignore;
import java.util.Random;

/**
 * @author Scott Fines
 * Created on: 6/7/13
 */
@Deprecated
public class StringEncodingMicroBenchmark {
	/*
    private static int numSerializations = 1000000;

    private static final String chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789?;:,.~'\"[]{}\\|/";
    private static final int maxStringLength=10000;

    public static void main(String... args) throws Exception{
        System.out.printf("Benchmarking StringRowKey%n");
        Pair<Stats,Stats> rowKeyStats = benchmarkRowKey();
        System.out.printf("-------%n");

        System.out.printf("Benchmarking StringEncoding%n");
        Pair<Stats,Stats> encoderStats = benchmarkEncoder();
        System.out.printf("-------%n");

        System.out.printf("Serialization Comparison%n");
        Stats encoderSerStats = encoderStats.getFirst();
        Stats rowSerStats = rowKeyStats.getFirst();
        double serializationTimeRatio = (double)encoderSerStats.getTotalTime()/rowSerStats.getTotalTime();
        System.out.printf("encoder.time/rowKey.time: %f%n",serializationTimeRatio);
        System.out.println("-------");

        System.out.printf("Deserialization Comparison%n");
        Stats encoderDeStats = encoderStats.getSecond();
        Stats rowDeStats = rowKeyStats.getSecond();
        double deserializationTimeRatio = (double)encoderDeStats.getTotalTime()/rowDeStats.getTotalTime();
        System.out.printf("encoder.time/rowKey.time: %f%n",deserializationTimeRatio);
        System.out.println("-------");
    }

    private static Pair<Stats, Stats> benchmarkEncoder() {
        Random random = new Random();

        Accumulator serAccum = TimingStats.uniformAccumulator();
        Accumulator deAccum = TimingStats.uniformAccumulator();
        serAccum.start();
        deAccum.start();
        long length=0l;
        long byteLength=0l;
        for(int i=0;i<numSerializations;i++){
            String next = getRandomString(random);
            long start = System.nanoTime();
            byte[] data = StringEncoding.toBytes(next, false);
            long end = System.nanoTime();
            serAccum.tick(1,end-start);
            byteLength+=data.length;

            start = System.nanoTime();
            String reverse = StringEncoding.getString(data, false);
            end = System.nanoTime();
            deAccum.tick(1,end-start);
            length +=reverse.length();
        }
        Stats serStats = serAccum.finish();
        Stats deStats = deAccum.finish();
        //print something to keep the loop alive
        System.out.println(length);
        System.out.println(byteLength);

        System.out.println(serStats);
        System.out.println(deStats);

        return Pair.newPair(serStats,deStats);
    }

    private static Pair<Stats,Stats> benchmarkRowKey() throws Exception{
        Random random = new Random();

        StringRowKey rowKey = new StringRowKey();

        Accumulator serAccum = TimingStats.uniformAccumulator();
        Accumulator deAccum = TimingStats.uniformAccumulator();
        serAccum.start();
        deAccum.start();
        long length=0l;
        long byteLength=0l;
        for(int i=0;i<numSerializations;i++){
            String next = getRandomString(random);
            long start = System.nanoTime();
            byte[] data = rowKey.serialize(next);
            long end = System.nanoTime();
            serAccum.tick(1,end-start);
            byteLength+=data.length;

            start = System.nanoTime();
            String reverse = (String)rowKey.deserialize(data);
            end = System.nanoTime();
            deAccum.tick(1,end-start);
            length +=reverse.length();
        }
        Stats serStats = serAccum.finish();
        Stats deStats = deAccum.finish();
        //print something to keep the loop alive
        System.out.println(length);
        System.out.println(byteLength);
        System.out.println(serStats);
        System.out.println(deStats);

        return Pair.newPair(serStats,deStats);
    }

    private static String getRandomString(Random random){
        char[] data = new char[random.nextInt(maxStringLength)+1];
        for(int pos=0;pos<data.length;pos++){
            data[pos] = chars.charAt(random.nextInt(chars.length()));
        }
        return new String(data);
    }
    */
}
