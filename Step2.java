package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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

public class Step2 {
    public static class MapperClass extends Mapper<Text, Text, Text, Text> {

        private Text newKey = new Text();

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            if (key.toString().equals("total"))
                context.write(key, value);
            else {
                String str = key.toString().substring(7);
                newKey.set(str);
                context.write(newKey, value);
            }
        }
    }
///// ("trigram: " + trigram + "," + "occurrences:" + sumOcc + "," + "part0:" + sumPart + "," + "part1:" + "," + "r:");
    public static class CombinerClass extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum_0 = 0;
            int sum_1 = 0;
            int sumOcc = 0;
            Text txt = new Text();
            if (!key.toString().equals("total")) {
                for (Text val : values) {
                    String[] spt = val.toString().split(",", 6);
                    sumOcc += Integer.parseInt(spt[1].substring(12));
                    sum_0 += Integer.parseInt(spt[2].substring(6));
                    sum_1 += Integer.parseInt(spt[3].substring(6));
                }
                txt.set("trigram: " + key.toString() + "," +"occurrences:" + sumOcc +","+ "part0:" + sum_0 + "," + "part1:" + sum_1 +","+"r:" );

                context.write(key, txt);
            } else {
                for (Text val : values)
                    context.write(key, val);

            }
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum_0 = 0;
            int sum_1 = 0;
            Text txt = new Text();
            if (!key.toString().equals("total")) {
                for (Text val : values) {
                    String[] spt = val.toString().split(",", 6);
                    sum_0 += Integer.parseInt(spt[2].substring(6));
                    sum_1 += Integer.parseInt(spt[3].substring(6));
                }
                txt.set("trigram: " + key.toString() + "," + "part0:" + sum_0 + "," + "part1:" + sum_1 + "," + "r:" + (sum_0 + sum_1));

                context.write(key, txt);
            } else {
                for (Text val : values)
                    context.write(key, val);

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
        //JOB-2
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "step2");
        job2.setJarByClass(Step2.class);
        job2.setMapperClass(Step2.MapperClass.class);
        job2.setPartitionerClass(Step2.PartitionerClass.class);
        job2.setCombinerClass(Step2.CombinerClass.class);
        job2.setReducerClass(Step2.ReducerClass.class);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);

        //map output <key,value>
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        //reduce output <key,value>
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(args[0])); //should be args[0]
        FileOutputFormat.setOutputPath(job2, new Path(args[1])); //should be args[1]
        job2.waitForCompletion(true) ;
    }

}
