package org.experiment.exp2.Q3;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MergeSort {
    /**
     * 输入多个文件，每个文件中的每行内容均为一个整数
     * 输出到一个新的文件中，输出的数据格式为每行两个整数，第一个数字为第二个整
     * 数的排序位次，第二个整数为原待排列的整数
     */
    //map 函数读取输入中的 value，将其转化成 IntWritable 类型，最后作为输出 key
    public static class Map extends Mapper<Object, Text, IntWritable, IntWritable> {
        private static IntWritable data = new IntWritable();

        public void map(Object key, Text value, Context context) throws
                IOException, InterruptedException {
            String text = value.toString();
            data.set(Integer.parseInt(text));
            context.write(data, new IntWritable(1));
        }
    }

    //reduce 函数将 map 输入的 key 复制到输出的 value 上，然后根据输入的 value-list

    public static class Reduce extends Reducer<IntWritable, IntWritable,
            IntWritable, IntWritable> {
        private static IntWritable line_num = new IntWritable(1);

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context
                context) throws IOException, InterruptedException {
            for (IntWritable val : values) {
                context.write(line_num, key);
                line_num = new IntWritable(line_num.get() + 1);
            }
        }
    }

    //自定义 Partition 函数，此函数根据输入数据的最大值和 MapReduce

    public static class Partition extends Partitioner<IntWritable, IntWritable> {
        public int getPartition(IntWritable key, IntWritable value, int num_Partition) {
            int Maxnumber = 65223;//int 型的最大数值
            int bound = Maxnumber / num_Partition + 1;
            int keynumber = key.get();
            for (int i = 0; i < num_Partition; i++) {
                if (keynumber < bound * (i + 1) && keynumber >= bound * i) {
                    return i;
                }
            }
            return -1;
        }
    }

    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://10.23.71.70:9000");
        conf.set("dfs.client.use.datanode.hostname", "true");
        Properties properties = System.getProperties();
        properties.setProperty("HADOOP_USER_NAME", "root");
        String[] otherArgs = new String[]{"/exp2_Q3", "/exp2_Q3/output"}; /* 直接设置输入参数
        String[] otherArgs = new String[]{"input", "output"}; /* 直接设置输入参数
         */
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in><out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Merge and sort");
        job.setJarByClass(MergeSort.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setPartitionerClass(Partition.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}