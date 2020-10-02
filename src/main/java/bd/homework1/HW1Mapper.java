package bd.homework1;

import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Маппер - парсит строку, получает IP-адрес и количество байт на запрос
 * Количество байт записывается в IpParams, остальные поля будут посчитаны позже
 */
public class HW1Mapper extends Mapper<Object, Text, Text, IpParams> {

    private Text word = new Text();
    private IpParams params = new IpParams();

    public static boolean checkCorrectIP(String IP)
    {
        String[] arr = IP.split("\\.");

        if (arr.length != 4)
            return false;
        for (String i : arr)
        {
            try
            {
                Integer.parseInt(i);
            }
            catch (Exception ex)
            {
                return false;
            }
        }
        return true;
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(" ");

        if (!checkCorrectIP(fields[0]))
        {
            context.getCounter(CounterType.MALFORMED_STRINGS).increment(1);
            return;
        }

        if (fields[8] != "-")
        {
            int bytes = 0;
            try
            {
                bytes = Integer.parseInt(fields[8]);
            }
            catch (Exception ex)
            {
                context.getCounter(CounterType.MALFORMED_STRINGS).increment(1);
                return;
            }

            params.setBytes(bytes);
        }

        word.set(fields[0]);
        context.write(word, params);
    }
}
