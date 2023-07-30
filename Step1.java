package org.example;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.*;

public class Step1 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        private Text word = new Text();
        private Text val = new Text();
        private Text total = new Text("total");
        private Text totalVal = new Text();
        final Set<String> stopWords = new HashSet<String>(Arrays.asList(
                "את", "לא", "של", "אני", "על", "זה", "עם", "כל", "הוא", "אם", "או", "גם", "יותר", "יש",
                "לי", "מה", "אבל", "פורום", "אז", "טוב", "רק", "כי", "שלי", "היה", "בפורום", "אין", "עוד", "היא",
                "אחד", "ב", "ל", "עד", "לך", "כמו", "להיות", "אתה", "כמה", "אנחנו", "הם", "כבר", "אנשים"
                , "אפשר", "תודה", "שלא", "אותו", "ה", "מאוד", "הרבה", "ולא", "ממש", "לו", "א", "מי", "חיים", "בית",
                "שאני", "יכול", "שהוא", "כך", "הזה", "איך", "היום", "קצת", "עכשיו", "שם", "בכל", "יהיה", "תמיד", "י",
                "שלך", "הכי", "ש", "בו", "לעשות", "צריך", "כן", "פעם", "לכם", "ואני", "משהו", "אל", "שלו", "שיש", "ו", "וגם",
                "אתכם", "אחרי", "בנושא", "כדי", "פשוט", "לפני", "שזה", "אותי", "אנו", "למה", "דבר", "כ", "כאן", "אולי", "טובים",
                "רוצה", "שנה", "בעלי", "החיים", "למען", "אתם", "מ", "בין", "יום", "זאת", "איזה", "ביותר", "לה", "אחת", "הכל",
                "הפורומים", "לכל", "אלא", "פה", "יודע", "שלום", "דקות", "לנו", "השנה", "דרך", "אדם", "נראה", "זו", "היחידה", "רוצים"
                , "בכלל", "טובה", "שלנו", "האם", "הייתי", "הלב", "היו", "ח", "שדרות", "בלי", "להם", "שאתה", "אותה", "מקום", "ואתם",
                "חלק", "בן", "בואו", "אחר", "האחת", "אותך", "כמובן", "בגלל", "באמת", "מישהו", "ילדים", "אותם", "הפורום", "טיפוח",
                "וזה", "ר", "שהם", "אך", "מזמין", "ישראל", "כוס", "זמן", "ועוד", "הילדים", "עדיין", "כזה", "עושה", "שום", "לקחת", "העולם"
                , "תפוז", "לראות", "לפורום", "וכל", "לקבל", "נכון", "יוצא", "לעולם", "גדול", "אפילו", "ניתן", "שני", "אוכל", "קשה",
                "משחק", "ביום", "ככה", "אמא", "בת", "השבוע", "נוספים", "לגבי", "בבית", "אחרת", "לפי"));


        public boolean goodTrigram(String str) {

            String[] trigram_words = str.split("\\s+");
            if (trigram_words.length != 3)
                return false;
            for (String s : trigram_words) {
                for (int i = 0; i < s.length(); i++)
                    if (s.charAt(i) == ',' || s.charAt(i) < 'א' || s.charAt(i) > 'ת')
                        return false;
                if (stopWords.contains(s))
                    return false;
            }
            return true;
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] str = value.toString().split("\t|\\t");
            if (str.length >= 3) {
                if (goodTrigram(str[0])) {
                    String trigram = str[0];
                    int occurrences;
                    try {
                        occurrences = Integer.parseInt(str[2]);
                    } catch (Exception e) {
                        System.out.println("error parsing string to int");
                        occurrences = -1;
                    }

                    if ((key.get() % 2) == 0) {
                        val.set("trigram: " + trigram + "," + "occurrences:" + occurrences + "," + "part0:" + occurrences + "," + "part1:" + "," + "r:");
                        word.set("part0: " + trigram);
                    } else {
                        val.set("trigram: " + trigram + "," + "occurrences:" + occurrences + "," + "part0:" + "," + "part1:" + occurrences + "," + "r:");
                        word.set("part1: " + trigram);
                    }
                    totalVal.set(Integer.toString(occurrences));

                    context.write(word, val);
                    context.write(total, totalVal);

                }
            }
        }
    }


    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            Text txt = new Text();
            if (!key.toString().equals("total")) {
                for (Text val : values) {
                    String[] spt = val.toString().split(",", 6);
                    sum += Integer.parseInt(spt[1].substring(12));
                }
                if (key.toString().substring(0, 6).equals("part0:"))
                    txt.set("trigram: " + key.toString().substring(7) + "," + "occurrences:" + sum + "," + "part0:" + sum + "," + "part1:0" + "," + "r:");
                else
                    txt.set("trigram: " + key.toString().substring(7) + "," + "occurrences:" + sum + "," + "part0:0" + "," + "part1:" + sum + "," + "r:");

                context.write(key, txt);
            } else {
                for (Text val : values)
                    sum += Integer.parseInt(val.toString());
                context.write(key, new Text(Integer.toString(sum)));

            }

        }
    }


    public static class CombinerClass extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sumOcc = 0;
            int sumPart = 0;
            int sum = 0;
            Text txt = new Text();
            if (!key.toString().equals("total")) {
                for (Text val : values) {
                    String[] spt = val.toString().split(",", 6);
                    sumOcc += Integer.parseInt(spt[1].substring(12));
                    if (key.toString().substring(0, 6).equals("part0:"))
                        sumPart += Integer.parseInt(spt[2].substring(6));
                    else
                        sumPart += Integer.parseInt(spt[3].substring(6));
                }
                String trigram = key.toString().substring(7);
                if (key.toString().substring(0, 6).equals("part0:"))
                    txt.set("trigram: " + trigram + "," + "occurrences:" + sumOcc + "," + "part0:" + sumPart + "," + "part1:" + "," + "r:");
                else
                    txt.set("trigram: " + trigram + "," + "occurrences:" + sumOcc + "," + "part0:" + "," + "part1:" + sumPart + "," + "r:");

                context.write(key, txt);
            } else {
                for (Text val : values)
                    sum += Integer.parseInt(val.toString());
                context.write(key, new Text(Integer.toString(sum)));

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
        //JOB-1
        Configuration conf1 = new Configuration();
        Job job = Job.getInstance(conf1, "step1");
        job.setJarByClass(Step1.class);
        job.setMapperClass(Step1.MapperClass.class);
        job.setPartitionerClass(Step1.PartitionerClass.class);
        job.setCombinerClass(Step1.CombinerClass.class);
        job.setReducerClass(Step1.ReducerClass.class);


        //map output <key,value>
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //reduce output <key,value>
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0])); //should be args[0]
        FileOutputFormat.setOutputPath(job, new Path(args[1])); //should be args[1]
        job.waitForCompletion(true);

    }


}