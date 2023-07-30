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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

public class Step6 {

    public static class Attributes {
        private String trigram;
        private double probability;


        public Attributes(String trigram, double probability) {
            this.trigram = trigram;
            this.probability = probability;
        }
    }

    public static class AttComperator implements Comparator<Attributes> {

        @Override
        public int compare(Attributes o1, Attributes o2) {
            if (o1.probability > o2.probability)
                return -1;
            else if (o1.probability < o2.probability) {
                return 1;
            } else
                return 0;
        }
    }

    public static class MapperClass extends Mapper<Text, Text, Text, Text> {


        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] spt = key.toString().split(" ");
            Text newKey = new Text(spt[0] + " " + spt[1]);
            Text newVal = new Text(key.toString() + "," + value.toString());
            context.write(newKey, newVal);

        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        public List<Attributes> lst = new ArrayList<>();
        public AttComperator cmp = new AttComperator();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            lst.clear();
            for (Text val : values) {
                String[] spt = val.toString().split(",");
                String trigram = spt[0];
                double pr = Double.parseDouble(spt[1]);
                Attributes toAdd = new Attributes(trigram, pr);
                lst.add(toAdd);
            }
            lst.sort(cmp);
            int len =lst.size();
            for (int i = 0; i < len; i++) {
                Attributes a = lst.remove(0);
                Text newKey = new Text(a.trigram);
                Text newVal = new Text(Double.toString(a.probability));
                context.write(newKey, newVal);

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
        //JOB-6
        Configuration conf6 = new Configuration();
        Job job6 = Job.getInstance(conf6, "step6");
        job6.setJarByClass(Step6.class);
        job6.setMapperClass(Step6.MapperClass.class);
        job6.setPartitionerClass(Step6.PartitionerClass.class);
        job6.setReducerClass(Step6.ReducerClass.class);
        job6.setInputFormatClass(KeyValueTextInputFormat.class);


        //map output <key,value>
        job6.setMapOutputKeyClass(Text.class);
        job6.setMapOutputValueClass(Text.class);

        //reduce output <key,value>
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(Text.class);

        job6.setInputFormatClass(SequenceFileInputFormat.class);
        job6.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job6, new Path(args[0])); //should be args[0]
        FileOutputFormat.setOutputPath(job6, new Path(args[1])); //should be args[1]
        job6.waitForCompletion(true);
    }
}
