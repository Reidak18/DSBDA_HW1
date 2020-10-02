package bd.homework1;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


@Log4j
public class MapReduceApplication {

    public static void main(String[] args) throws Exception {

        if (args.length < 2)
        {
            throw new RuntimeException("You should specify input and output folders!");
        }
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "requests statistics");
        job.setJarByClass(IpParams.class);
        job.setMapperClass(HW1Mapper.class);
        job.setCombinerClass(HW1Combiner.class);
        job.setReducerClass(HW1Reducer.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IpParams.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path outputDirectory = new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputDirectory);
        log.info("=====================JOB STARTED=====================");
        long start_time = System.currentTimeMillis();
        job.waitForCompletion(true);
        long work_time = System.currentTimeMillis() - start_time;
        log.info("=====================JOB ENDED=====================");
        // проверяем статистику по счётчикам
        Counter counter = job.getCounters().findCounter(CounterType.MALFORMED_STRINGS);
        log.info("=====================COUNTERS " + counter.getName() + ": " + counter.getValue() + "=====================");
        log.info("Time: " + String.valueOf(work_time));
    }
}
