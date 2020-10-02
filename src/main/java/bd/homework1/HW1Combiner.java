package bd.homework1;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.Text;

import java.io.IOException;

public class HW1Combiner extends Reducer<Text, IpParams, Text, IpParams>
{
    private IpParams globalParams = new IpParams();

    @Override
    public void reduce(Text key, Iterable<IpParams> values, Context context) throws IOException, InterruptedException {
        int globalBytesSum = 0;
        int globalRequestCount = 0;

        for(IpParams i : values)
        {
            globalBytesSum += i.bytesSum.get();
            globalRequestCount++;
        }

        globalParams.set(globalBytesSum, globalRequestCount, 0.0f);
        context.write(key, globalParams);
    }
}
