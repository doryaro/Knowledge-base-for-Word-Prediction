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

public class Step3 {
    public static class MapperClass extends Mapper<Text, Text, Text, Text> {

        private Text newKey1 = new Text();
        private Text newKey2 = new Text();


        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            if (key.toString().equals("total"))
                context.write(key, value);
            else {
                context.write(key, value);
                String[] spt = value.toString().split(",", 6);


                //calculating Ns
                String x = spt[1].substring(6);
                Text Nx0 = new Text("Ns- N" + x + "(0):");

                String y = spt[2].substring(6);
                Text Ny1 = new Text("Ns- N" + y + "(1):");

                context.write(Nx0, new Text("1"));
                context.write(Ny1, new Text("1"));


                //calculating Ts
                x = spt[1].substring(6);
                Text Tx0 = new Text("Ts- T" + x + "(0):");

                y = spt[2].substring(6);
                Text Ty1 = new Text("Ts- T" + y + "(1):");

                context.write(Tx0, new Text(spt[2].substring(6)));
                context.write(Ty1, new Text(spt[1].substring(6)));

            }
        }
    }

    public static class CombinerClass extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            if (key.toString().substring(0, 3).equals("Ts-") || key.toString().substring(0, 3).equals("Ns-")) {
                int sum = 0;
                for (Text val : values)
                    sum += Integer.parseInt(val.toString());
                Text newVal = new Text(Integer.toString(sum));
                context.write(key, newVal);
            } else
                for (Text val : values) {
                    context.write(key, val);
                }

        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {


            Text txt = new Text();
            if (key.toString().equals("total")) {
                for (Text val : values)
                    context.write(key, val);

            } else if (key.toString().substring(0, 3).equals("Ts-") || key.toString().substring(0, 3).equals("Ns-")) {
                int sum = 0;
                for (Text val : values)
                    sum += Integer.parseInt(val.toString());
                Text newVal = new Text(Integer.toString(sum));
                context.write(key, newVal);

            } else {
                for (Text val : values) {
                    context.write(key, val);
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
        //JOB-3
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "step3");
        job3.setJarByClass(Step3.class);
        job3.setMapperClass(Step3.MapperClass.class);
        job3.setPartitionerClass(Step3.PartitionerClass.class);
        job3.setReducerClass(Step3.ReducerClass.class);
        job3.setCombinerClass(Step3.CombinerClass.class);
        job3.setInputFormatClass(KeyValueTextInputFormat.class);


        //map output <key,value>
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);

        //reduce output <key,value>
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job3, new Path(args[0])); //should be args[0]
        FileOutputFormat.setOutputPath(job3, new Path(args[1])); //should be args[1]
        job3.waitForCompletion(true);
    }


}
