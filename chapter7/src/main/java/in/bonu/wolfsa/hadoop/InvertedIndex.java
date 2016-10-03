package in.bonu.wolfsa.hadoop;


import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;


public class InvertedIndex {

    public static class InvertedIndexMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {


        private final static Text word = new Text();
        private final static Text location = new Text();

        public void map(LongWritable key, Text val,
                        OutputCollector output, Reporter reporter)
                throws IOException {
            FileSplit fileSplit = (FileSplit)reporter.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            location.set(fileName);

            String line = val.toString();
            StringTokenizer itr = new StringTokenizer(line.toLowerCase());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                output.collect(word, location);
            }
        }
    }

    public static class InvertedIndexReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterator values,
                           OutputCollector output, Reporter reporter)
                throws IOException {
            boolean first = true;
            StringBuilder toReturn = new StringBuilder();
            while (values.hasNext()){
                if (!first)
                    toReturn.append(", ");
                first=false;
                toReturn.append(values.next().toString());
            }

            output.collect(key, new Text(toReturn.toString()));
        }
    }


    public static void run(String input, String output){
        JobClient client = new JobClient();
        JobConf conf = new JobConf(InvertedIndex.class);

        conf.setJobName("InvertedIndex");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        conf.setMapperClass(InvertedIndexMapper.class);
        conf.setReducerClass(InvertedIndexReducer.class);

        client.setConf(conf);

        try {
            JobClient.runJob(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * The actual main() method for our program; this is the
     * "driver" for the MapReduce job.
     */
    public static void main(String[] args) throws Exception {
        if( args.length != 2 ){
            System.err.println("InvertedIndex <input_dir> <output_dir>");
        }else{
            run(args[0], args[1]);
        }
    }
}