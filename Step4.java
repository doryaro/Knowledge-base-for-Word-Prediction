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

public class Step4 {

    public static class MapperClass extends Mapper<Text, Text, Text, Text> {

        private Text newKey = new Text();
        private Text newValue = new Text();


        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String r;
            if (key.toString().equals("total"))
                context.write(key, value);
            else if (key.toString().substring(0, 3).equals("Ts-") || key.toString().substring(0, 3).equals("Ns-")) {
                r = key.toString().substring(5, key.toString().indexOf('('));
                newKey.set("r=" + r);
                newValue.set(key.toString().substring(4) + value.toString());
                context.write(newKey, newValue);
            } else {
                String[] spt = value.toString().split(",", 8);
                r = spt[3].substring(2);
                newKey.set("r=" + r);
                newValue.set(value);
                context.write(newKey, newValue);

            }
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        private HashMap<String, String> trigrams = new HashMap<>();
        private HashMap<String, String> Tr0 = new HashMap<>();
        private HashMap<String, String> Tr1 = new HashMap<>();
        private HashMap<String, String> Nr0 = new HashMap<>();
        private HashMap<String, String> Nr1 = new HashMap<>();


        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            trigrams.clear();
            Nr0.clear();
            Nr1.clear();
            Tr0.clear();
            Tr1.clear();
            if (key.toString().equals("total")) {
                for (Text val : values)
                    context.write(key, val);

            } else {
                for (Text val : values) {
                    if (val.toString().substring(0, 1).equals("N")) {
                        int from = val.toString().indexOf('(') + 1;
                        int to = val.toString().indexOf(')');
                        if (val.toString().substring(from, to).equals("0"))
                            Nr0.put(key.toString(), val.toString());
                        else
                            Nr1.put(key.toString(), val.toString());

                    } else if (val.toString().substring(0, 1).equals("T")) {
                        int from = val.toString().indexOf('(') + 1;
                        int to = val.toString().indexOf(')');
                        if (val.toString().substring(from, to).equals("0"))
                            Tr0.put(key.toString(), val.toString());
                        else
                            Tr1.put(key.toString(), val.toString());
                    } else {
                        String[] spt = val.toString().split(",");
                        String trigram = spt[0].substring(9);
                        trigrams.put(trigram, val.toString());
                    }
                }

                trigrams.forEach((Key, Value) -> {
                    Text newKey = new Text();
                    Text newValue = new Text();
                    String[] spt = Value.split(",", 8);
                    String r = spt[3].substring(2);
                    String rToCheck = "r=" + r;
                    String nr0;
                    String nr1;
                    String tr0;
                    String tr1;
                    if (Nr0.containsKey(rToCheck))
                        nr0 = Nr0.get(rToCheck);
                    else
                        nr0 = "N" + r + "(0):0";
                    if (Nr1.containsKey(rToCheck))
                        nr1 = Nr1.get(rToCheck);
                    else
                        nr1 = "N" + r + "(1):0";
                    if (Tr0.containsKey(rToCheck))
                        tr0 = Tr0.get(rToCheck);
                    else
                        tr0 = "T" + r + "(0):0";
                    if (Tr1.containsKey(rToCheck))
                        tr1 = Tr1.get(rToCheck);
                    else
                        tr1 = "T" + r + "(1):0";

                    newKey.set(Key);
                    newValue.set(Value + "," + nr0 + "," + nr1 + "," + tr0 + "," + tr1);
                    try {
                        context.write(newKey, newValue);
                    } catch (IOException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                });
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

        //JOB-4
        Configuration conf4 = new Configuration();
        Job job4 = Job.getInstance(conf4, "step4");
        job4.setJarByClass(Step4.class);
        job4.setMapperClass(Step4.MapperClass.class);
        job4.setPartitionerClass(Step4.PartitionerClass.class);
        job4.setReducerClass(Step4.ReducerClass.class);
        job4.setInputFormatClass(KeyValueTextInputFormat.class);


        //map output <key,value>
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);

        //reduce output <key,value>
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);

        job4.setInputFormatClass(SequenceFileInputFormat.class);
        job4.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job4, new Path(args[0])); //should be args[0]
        FileOutputFormat.setOutputPath(job4, new Path(args[1])); //should be args[1]
        job4.waitForCompletion(true);
    }

}
