import bd.homework1.HW1Combiner;
import bd.homework1.IpParams;
import bd.homework1.HW1Mapper;
import bd.homework1.HW1Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

/**
 * Тесты маппера, комбайнера и редьюсера
 */
public class MapReduceTest {

    private MapDriver<Object, Text, Text, IpParams> mapDriver;
    private ReduceDriver<Text, IpParams, Text, IpParams> reduceDriver;
    private MapReduceDriver<Object, Text, Text, IpParams, Text, IpParams> mapReduceDriver;

    private final String testIP = "94.143.40.178 - - [12/Jul/2020:14:27:11 +0100] \\\"GET https://mysite.com\\\" 202 2701 \\\"-\\\"Mozilla/5.0 (Linux; U; Android 6.0; zh-CN; MZ-U10 Build/MRA58K) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/57.0.2987.108 MZBrowser/8.2.210-2020061519 UWS/2.15.0.4 Mobile Safari/537.36\";";

    private ReduceDriver<Text, IpParams, Text, IpParams> combinerDriver;

    @Before
    public void setUp()
    {
        HW1Mapper mapper = new HW1Mapper();
        HW1Combiner combiner = new HW1Combiner();
        HW1Reducer reducer = new HW1Reducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        combinerDriver = ReduceDriver.newReduceDriver(combiner);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException
    {
        IpParams params = new IpParams();
        params.setBytes(2701);
        mapDriver
                .withInput(new LongWritable(), new Text(testIP))
                .withOutput(new Text("94.143.40.178"), params)
                .runTest();
    }

    @Test
    public void testCombiner() throws IOException
    {
        IpParams valueParams = new IpParams();
        valueParams.setBytes(2701);

        IpParams resultParam = new IpParams();
        resultParam.set(2701, 1, 0);

        List<IpParams> values = new ArrayList<IpParams>();
        values.add(valueParams);
        combinerDriver
                .withInput(new Text("94.143.40.178"), values)
                .withOutput(new Text("94.143.40.178"), resultParam)
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<IpParams> values = new ArrayList<IpParams>();
        IpParams params1 = new IpParams();
        params1.setBytes(10);
        IpParams params2 = new IpParams();
        params2.setBytes(20);
        values.add(params1);
        values.add(params2);

        IpParams resultParams = new IpParams();
        resultParams.set(30, 2, 15);
        reduceDriver
                .withInput(new Text("94.143.40.178"), values)
                .withOutput(new Text("94.143.40.178"), resultParams)
                .runTest();
    }

    @Test
    public void testMapReduce() throws IOException
    {
        IpParams result = new IpParams();
        result.set(2701*2, 2, 2701);
        mapReduceDriver
                .withInput(new LongWritable(), new Text(testIP))
                .withInput(new LongWritable(), new Text(testIP))
                .withOutput(new Text("94.143.40.178"), result)
                .runTest();
    }

    @Test
    public void testCheckCorrectIP() throws IOException
    {
        assertEquals(HW1Mapper.checkCorrectIP("94.143.40.178"), true);
        assertEquals(HW1Mapper.checkCorrectIP("1.1.1.1"), true);
        assertEquals(HW1Mapper.checkCorrectIP("1.1.1.1.1"), false);
        assertEquals(HW1Mapper.checkCorrectIP("1.1.1.1.y"), false);
        assertEquals(HW1Mapper.checkCorrectIP("y.1.1.1.1"), false);
        assertEquals(HW1Mapper.checkCorrectIP("1.1.y.1.1"), false);
        assertEquals(HW1Mapper.checkCorrectIP("1a.1.1.1"), false);
        assertEquals(HW1Mapper.checkCorrectIP("1.1a.1.1"), false);
        assertEquals(HW1Mapper.checkCorrectIP("1.1.1a.1"), false);
        assertEquals(HW1Mapper.checkCorrectIP("1.1.1.1a"), false);
        assertEquals(HW1Mapper.checkCorrectIP("dsfsafda"), false);
        assertEquals(HW1Mapper.checkCorrectIP("..."), false);
    }
}
