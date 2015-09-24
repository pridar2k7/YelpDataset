import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.TreeMap;

/**
* Created by priyadarshini on 9/22/15.
*/
public class Yelp {
    public static class Map
            extends Mapper<LongWritable, Text, Text, Text> {
        private Text rating = new Text();
        private Text businessId = new Text(); // type of output key

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] mydata = value.toString().split("\\^");
            businessId.set(mydata[2]); // set word as each input keyword
            rating.set(mydata[3]);
            context.write(businessId, rating); // create a pair <keyword, 1>
        }
    }

    public static class Top10Mapper
            extends Mapper<LongWritable, Text, Text, Text> {
        private Text rating = new Text();
        private Text businessId = new Text(); // type of output key

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] mydata = value.toString().split("\\^");
            businessId.set(mydata[2]); // set word as each input keyword
            rating.set(mydata[3]);
            context.write(businessId, rating); // create a pair <keyword, 1>
        }
    }

    public static class BusinessMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        private Text rating = new Text();
        private Text businessId = new Text(); // type of output key

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] mydata = value.toString().split("\\^");
            businessId.set(mydata[2]); // set word as each input keyword
            rating.set(mydata[3]);
            context.write(businessId, rating); // create a pair <keyword, 1>
        }
    }

    public static class Combine
            extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sum = 0;

            int count = 0;
            for (Text val : values) {
                sum += Double.parseDouble(val.toString());
                count++;
            }
            result.set(Double.toString(sum) + "_" + Integer.toString(count));
            context.write(key, result);
        }
    }

    public static class Reduce
            extends Reducer<Text, Text, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
        TreeMap<Double, ArrayList<Text>> mapToSort = new TreeMap<Double, ArrayList<Text>>();


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double totalSum = 0;
            int totalCount = 0;
            for (Text val : values) {
                String[] concatenatedValue = val.toString().split("_");
                totalSum += Double.parseDouble(concatenatedValue[0]);
                totalCount += Double.parseDouble(concatenatedValue[1]);
            }
            double average = totalSum / (double) totalCount;
            result.set(average);

            ArrayList<Text> list;
            if((list=mapToSort.get(average))!=null){
                list.add(new Text(key.toString()+"_"+average+"_"+totalCount));
                mapToSort.put(average, list);
            }
            else{
                ArrayList<Text> newList=new ArrayList<Text>();
                newList.add(new Text(key.toString()+"_"+average+"_"+totalCount));
                mapToSort.put(average, newList);
            }
        }

        protected void cleanup(Reducer.Context context) throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            int count=0;
            for(ArrayList<Text> mapValues:mapToSort.descendingMap().values()){
                sortRecordsByValue(mapValues);
                if(count>=10){
                    break;
                }
                for(Text t:mapValues){
                    if(count>=10){
                        break;
                    }
                    context.write(t.toString().split("_")[0], new Text(t.toString().split("_")[1]));
                    count++;
                }
            }
        }

        void sortRecordsByValue(ArrayList<Text> recordsList){
            Collections.sort(recordsList,new Comparator<Text>() {
                public int compare(Text text1, Text text2) {
                    return Integer.parseInt(text2.toString().split("_")[2]) - Integer.parseInt(text1.toString().split("_")[2]);
                }
            });
        }

    }

    public static class JoinReducer
            extends Reducer<Text, Text, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
        TreeMap<Double, ArrayList<Text>> mapToSort = new TreeMap<Double, ArrayList<Text>>();


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double totalSum = 0;
            int totalCount = 0;
            for (Text val : values) {
                String[] concatenatedValue = val.toString().split("_");
                totalSum += Double.parseDouble(concatenatedValue[0]);
                totalCount += Double.parseDouble(concatenatedValue[1]);
            }
            double average = totalSum / (double) totalCount;
            result.set(average);

            ArrayList<Text> list;
            if((list=mapToSort.get(average))!=null){
                list.add(new Text(key.toString()+"_"+average+"_"+totalCount));
                mapToSort.put(average, list);
            }
            else{
                ArrayList<Text> newList=new ArrayList<Text>();
                newList.add(new Text(key.toString()+"_"+average+"_"+totalCount));
                mapToSort.put(average, newList);
            }
        }

        protected void cleanup(Reducer.Context context) throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            int i=0;
            for(ArrayList<Text> listt:mapToSort.descendingMap().values()){
                sortRecordsByValue(listt);
                if(i>=10){
                    break;
                }
                for(Text t:listt){
                    if(i>=10){
                        break;
                    }
                    context.write(t.toString().split("_")[0], new Text(t.toString().split("_")[1]));
                    i++;
                }
            }
        }

        void sortRecordsByValue(ArrayList<Text> recordsList){
            Collections.sort(recordsList,new Comparator<Text>() {
                public int compare(Text text1, Text text2) {
                    return Integer.parseInt(text2.toString().split("_")[2]) - Integer.parseInt(text1.toString().split("_")[2]);
                }
            });
        }

    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 4) {
            System.err.println("Usage: WordCount <business> <review> <user> <output>");
            System.exit(2);
        }

        Job job = new Job(conf, "yelpSecond");
        job.setJarByClass(Yelp.class);

        job.setReducerClass(Reduce.class);
        job.setCombinerClass(Combine.class);
        job.setMapperClass(Map.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

        if(job.waitForCompletion(true)){
            Configuration conf2=new Configuration();
            Job job2=new Job(conf2, "Top10BusinessDetails");
            job2.setJarByClass(Yelp.class);
            job2.setReducerClass(JoinReducer.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            MultipleInputs.addInputPath(job2, new Path(otherArgs[2]), TextInputFormat.class, Top10Mapper.class);
            MultipleInputs.addInputPath(job2, new Path(otherArgs[0]), TextInputFormat.class, BusinessMapper.class);
            FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));
            System.exit(job2.waitForCompletion(true)? 0 : 1);
        }
    }
}
