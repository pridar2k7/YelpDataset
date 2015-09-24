//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.Comparator;
//import java.util.TreeMap;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.GenericOptionsParser;
//
//public class YelpSample {
//
//    /**
//     * AverageMapper, AverageCombiner, AverageReducer are used to determine the top 10 businesses based on average ratings and number of ratings.
//     *
//     */
//    public static class AverageMapper extends Mapper<LongWritable, Text, Text, Text> {
//
//        @Override
//        protected void map(
//                LongWritable key,
//                Text value,
//                Mapper<LongWritable, Text, Text, Text>.Context context)
//                throws IOException, InterruptedException {
//            // TODO Auto-generated method stub
//            String[] input = value.toString().trim().split("::");
//            if(!input[2].trim().equalsIgnoreCase("NaN") && !input[20].trim().equalsIgnoreCase("NaN")){
//                context.write(new Text(input[2].trim()), new Text(input[20].trim()+"::1"));
//            }
//        }
//    }
//
//
//    public static class AverageCombiner extends Reducer<Text, Text, Text, Text> {
//
//        @Override
//        protected void reduce(
//                Text key,
//                Iterable<Text> values,
//                Reducer<Text, Text, Text, Text>.Context context)
//                throws IOException, InterruptedException {
//            // TODO Auto-generated method stub
//            int countBig=0;
//            float sumBig=0;
//            for(Text value : values){
//                String[] val=value.toString().split("::");
//                float average = Float.parseFloat(val[0]);
//                int count = Integer.parseInt(val[1]);
//                sumBig += average * count;
//                countBig += count;
//            }
//            context.write(key, new Text((sumBig/countBig)+"::"+countBig));
//        }
//    }
//
//    public static class AverageReducer extends Reducer<Text, Text, Text, Text> {
//
//        TreeMap<Float, ArrayList<Text>> mapToSort = new TreeMap<Float, ArrayList<Text>>();
//        @Override
//        protected void reduce(
//                Text key,
//                Iterable<Text> values,
//                Reducer<Text, Text, Text, Text>.Context context)
//                throws IOException, InterruptedException {
//            // TODO Auto-generated method stub
//            int countBig=0;
//            float sumBig=0;
//            for(Text value : values){
//                String[] val=value.toString().split("::");
//                float average = Float.parseFloat(val[0]);
//                int count = Integer.parseInt(val[1]);
//                sumBig += average * count;
//                countBig += count;
//            }
//            float avgBig = sumBig/countBig;
//            ArrayList<Text> list;
//            if((list=mapToSort.get(avgBig))!=null){
//                list.add(new Text(key.toString()+"::"+avgBig+"::"+countBig));
//                mapToSort.put(avgBig, list);
//            }
//            else{
//                ArrayList<Text> newList=new ArrayList<Text>();
//                newList.add(new Text(key.toString()+"::"+avgBig+"::"+countBig));
//                mapToSort.put(avgBig, newList);
//            }
//        }
//
//        @Override
//        protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
//                throws IOException, InterruptedException {
//            // TODO Auto-generated method stub
//            int i=0;
//            for(ArrayList<Text> listt:mapToSort.descendingMap().values()){
//                sortRecordsByValue(listt);
//                if(i>=10){
//                    break;
//                }
//                for(Text t:listt){
//                    if(i>=10){
//                        break;
//                    }
//                    context.write(t, new Text(" "));
//                    i++;
//                }
//            }
//        }
//
//        void sortRecordsByValue(ArrayList<Text> recordsList){
//            Collections.sort(recordsList,new Comparator<Text>() {
//                public int compare(Text text1, Text text2) {
//                    int count1=Integer.parseInt(text1.toString().split("::")[2]);
//                    int count2=Integer.parseInt(text2.toString().split("::")[2]);
//                    return count2-count1;
//                }
//            });
//        }
//    }
//
//    /**
//     * Mapper for top 10 businesses
//     */
//    public static class Top10BusinessJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
//
//        @Override
//        protected void map(LongWritable key, Text value,
//                           Mapper<LongWritable, Text, Text, Text>.Context context)
//                throws IOException, InterruptedException {
//            // TODO Auto-generated method stub
//            String[] input = value.toString().trim().split("::");
//            if(input.length==3){
//                context.write(new Text(input[0].trim()), new Text("TP"+value.toString()));
//            }
//        }
//    }
//
//    /**
//     * Mapper for all businesses
//     */
//    public static class AllBusinessJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
//
//        @Override
//        protected void map(LongWritable key, Text value,
//                           Mapper<LongWritable, Text, Text, Text>.Context context)
//                throws IOException, InterruptedException {
//            // TODO Auto-generated method stub
//            String[] input = value.toString().trim().split("::");
//            context.write(new Text(input[2].trim()), new Text("AL"+input[2].trim()+"::"+input[3].trim()+"::"+input[10].trim()));
//        }
//    }
//
//    /**
//     * Reducer to perform join
//     */
//    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
//
//        ArrayList<String> topBusiness = new ArrayList<String>();
//        ArrayList<String> businessDetail = new ArrayList<String>();
//        @Override
//        protected void reduce(Text key, Iterable<Text> values,
//                              Reducer<Text, Text, Text, Text>.Context context)
//                throws IOException, InterruptedException {
//            // TODO Auto-generated method stub
//            topBusiness.clear();
//            businessDetail.clear();
//            for(Text value : values){
//
//                String input=value.toString().trim();
//                if(input.substring(0, 2).equals("TP")){
//                    topBusiness.add(input.substring(2));
//                }
//                else if(input.substring(0, 2).equals("AL")){
//                    businessDetail.add(input.substring(2));
//                }
//            }
//            executeJoinLogic(context);
//        }
//
//        /**
//         * Performing a left outer join
//         * @throws InterruptedException
//         * @throws IOException
//         */
//        void executeJoinLogic(Context context) throws IOException, InterruptedException{
//
//            for(String business : topBusiness){
//                if(!businessDetail.isEmpty()){
//                    for(String detail : businessDetail){
//                        context.write(new Text(business),new Text(detail));
//                    }
//                }
//                else{
//                    context.write(new Text(business), new Text("unknown"));
//                }
//            }
//        }
//    }
//
//    @SuppressWarnings("deprecation")
//    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
//        Configuration conf=new Configuration();
//        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//        if(otherArgs.length!=4){
//            System.err.println("Error! Insufficient arguments. Provide arguments <Input directory:business.csv> <Input directory:review.csv> <Intermediate result directory> <Output directory>");
//            System.exit(2);
//        }
//        Job job=new Job(conf, "Top10Business");
//        job.setJarByClass(Top10BusinessDetails.class);
//        job.setMapperClass(AverageMapper.class);
//        job.setCombinerClass(AverageCombiner.class);
//        job.setReducerClass(AverageReducer.class);
//        job.setNumReduceTasks(1);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
//        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
//        if(job.waitForCompletion(true)){
//            Configuration conf2=new Configuration();
//            Job job2=new Job(conf2, "Top10BusinessDetails");
//            job2.setJarByClass(Top10BusinessDetails.class);
//            job2.setReducerClass(JoinReducer.class);
//            job2.setOutputKeyClass(Text.class);
//            job2.setOutputValueClass(Text.class);
//            MultipleInputs.addInputPath(job2, new Path(otherArgs[2]), TextInputFormat.class, Top10BusinessJoinMapper.class);
//            MultipleInputs.addInputPath(job2, new Path(otherArgs[0]), TextInputFormat.class, AllBusinessJoinMapper.class);
//            FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));
//            System.exit(job2.waitForCompletion(true)? 0 : 1);
//        }
//    }
//
//}