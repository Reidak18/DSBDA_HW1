package bd.homework1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Редьюсер: суммирует все байты, полученные от маппера для получения суммарного числа байт;
 * считает число запросов для вычисления среднего кол-ва байтов на запрос;
 * вычисляет среднее кол-во байтов на запрос
 */
public class HW1Reducer extends Reducer<Text, IpParams, Text, IpParams> {

    private IpParams finalParams = new IpParams();
    @Override
    protected void reduce(Text key, Iterable<IpParams> values, Context context) throws IOException, InterruptedException {
        int bytesSum = 0;
        int requestCount = 0;

        for (IpParams i : values)
        {
            bytesSum += i.bytesSum.get();
            requestCount++;
        }

        float bytesAverage = 0;
        if (requestCount == 0)
            bytesAverage = 0;
        else
            bytesAverage = (float)bytesSum / requestCount;

        finalParams.set(bytesSum, requestCount, bytesAverage);
        context.write(key, finalParams);
    }
}
