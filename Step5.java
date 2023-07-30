package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Step5 {

    public static HashMap<String, String> Ts = new HashMap<>();
    public static String N;

    public static class MapperClass extends Mapper<Text, Text, Text, Text> {


        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            if (!key.toString().equals("total"))

                context.write(key, value);
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private Text newVal = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            for (Text val : values) {
                String[] spt = val.toString().split(",", 10);
                double Nr0 = Double.parseDouble(spt[4].substring(spt[4].indexOf(':') + 1));
                double Nr1 = Double.parseDouble(spt[5].substring(spt[5].indexOf(':') + 1));
                double Tr0 = Double.parseDouble(spt[6].substring(spt[6].indexOf(':') + 1));
                double Tr1 = Double.parseDouble(spt[7].substring(spt[7].indexOf(':') + 1));
                double N = 	163471963;
                double p;
                if (Nr0 + Nr1 != 0) {
                    p = (Tr0 + Tr1) / ((Nr0 + Nr1) * N);
                    context.write(key, new Text(Double.toString(p)));
                }
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;

        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //JOB-5
        Configuration conf5 = new Configuration();
        Job job5 = Job.getInstance(conf5, "step5");
        job5.setJarByClass(Step5.class);
        job5.setMapperClass(Step5.MapperClass.class);
        job5.setPartitionerClass(Step5.PartitionerClass.class);
        job5.setReducerClass(Step5.ReducerClass.class);
        job5.setInputFormatClass(KeyValueTextInputFormat.class);


        //map output <key,value>
        job5.setMapOutputKeyClass(Text.class);
        job5.setMapOutputValueClass(Text.class);

        //reduce output <key,value>
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);

        job5.setInputFormatClass(SequenceFileInputFormat.class);
        job5.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job5, new Path(args[0])); //should be args[0]
        FileOutputFormat.setOutputPath(job5, new Path(args[1])); //should be args[1]
        job5.waitForCompletion(true);
    }
}
