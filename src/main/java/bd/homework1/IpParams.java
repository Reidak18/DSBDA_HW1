package bd.homework1;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Класс для хранения параметров IP-адреса - суммарное кол-во байт, количество запросов, среднее кол-во байт на запрос
 */

public class IpParams implements Writable {
    public IntWritable requstCount = new IntWritable(1);
    public IntWritable bytesSum = new IntWritable(0);
    public FloatWritable bytesAverage = new FloatWritable(0.0f);

    public void setBytes(int bytes)
    {
        bytesSum.set(bytes);
    }

    public void set(int bytes, int requests, float average)
    {
        bytesSum.set(bytes);
        requstCount.set(requests);
        bytesAverage.set(average);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        bytesSum.write(dataOutput);
        requstCount.write(dataOutput);
        bytesAverage.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        bytesSum.readFields(dataInput);
        requstCount.readFields(dataInput);
        bytesAverage.readFields(dataInput);
    }

    @Override
    public String toString()
    {
        return bytesSum.toString() + ", " + requstCount.toString() + ", " + bytesAverage.toString();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof IpParams)
        {
            IpParams other = (IpParams)obj;
            return bytesSum.equals((other.bytesSum)) &&
                    requstCount.equals(other.requstCount) &&
                    bytesAverage.equals(other.bytesAverage);
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return this.hashCode();
    }
}
