import bd.homework1.IpParams;
import bd.homework1.CounterType;
import bd.homework1.HW1Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Тесты счетчика неправильных строк
 */
public class CountersTest {

    private MapDriver<Object, Text, Text, IpParams> mapDriver;

    private final String testMalformedIP = "mama mila ramu";
    private final String testIP = "94.143.40.178 - - [12/Jul/2020:14:27:11 +0100] \\\"GET https://mysite.com\\\" 202 2701 \\\"-\\\"Mozilla/5.0 (Linux; U; Android 6.0; zh-CN; MZ-U10 Build/MRA58K) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/57.0.2987.108 MZBrowser/8.2.210-2020061519 UWS/2.15.0.4 Mobile Safari/537.36\";";

    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapperCounterOne() throws IOException
    {
        mapDriver
                .withInput(new LongWritable(), new Text(testMalformedIP))
                .runTest();
        assertEquals("Expected 1 counter increment", 1, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED_STRINGS).getValue());
    }

    @Test
    public void testMapperCounterZero() throws IOException
    {
        IpParams params = new IpParams();
        params.setBytes(2701);
        mapDriver
                .withInput(new LongWritable(), new Text(testIP))
                .withOutput(new Text("94.143.40.178"), params)
                .runTest();
        assertEquals("Expected 1 counter increment", 0, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED_STRINGS).getValue());
    }

    @Test
    public void testMapperCounters() throws IOException
    {
        IpParams params = new IpParams();
        params.setBytes(2701);
        mapDriver
                .withInput(new LongWritable(), new Text(testIP))
                .withInput(new LongWritable(), new Text(testMalformedIP))
                .withInput(new LongWritable(), new Text(testMalformedIP))
                .withOutput(new Text("94.143.40.178"), params)
                .runTest();

        assertEquals("Expected 2 counter increment", 2, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED_STRINGS).getValue());
    }
}

