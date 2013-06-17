package demo;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.io.LongWritable;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.KijiMapReduceJob;
import org.kiji.mapreduce.KijiReducer;
import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.mapreduce.gather.KijiGatherJobBuilder;
import org.kiji.mapreduce.gather.KijiGatherer;
import org.kiji.mapreduce.output.SequenceFileMapReduceJobOutput;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

public class TestGatherReducer extends KijiClientTest {
  private KijiURI mKijiURI;

  private static final boolean FAKE = true;

  @Before
  public void setup() throws Exception {
    if (FAKE) {
      mKijiURI = getKiji().getURI();
      new InstanceBuilder(getKiji())
          .withTable(KijiTableLayouts.getLayout("users.json"))
              .withRow("mwallace")
                  .withFamily("info")
                      .withQualifier("full_name")
                          .withValue("Marsellus Wallace")
                      .withQualifier("address")
                          .withValue(1000L, "Los Angeles")
                          .withValue(2000L, "San Francisco")
              .withRow("vvega")
                  .withFamily("info")
                      .withQualifier("full_name")
                          .withValue("Vincent Vega")
                      .withQualifier("address")
                          .withValue("San Francisco")
              .withRow("jwinnfield")
                  .withFamily("info")
                      .withQualifier("full_name")
                          .withValue("Jules Winnfield")
                      .withQualifier("address")
                          .withValue("Los Angeles")
              .withRow("bcoolidge")
                  .withFamily("info")
                      .withQualifier("full_name")
                          .withValue("Butch Coolidge")
          .build();
    } else {
      mKijiURI = KijiURI.newBuilder("kiji://localhost:2181/default").build();
    }
  }

  // -----------------------------------------------------------------------------------------------

  /** Emits pairs (# of addresses, # of users with that many addresses). */
  public static final class Gatherer extends KijiGatherer<LongWritable, LongWritable> {
    private static final Logger LOG = LoggerFactory.getLogger(TestGatherReducer.class);

    public static enum Counters { NO_ADDRESS, NO_ZIP_CODE }

    @Override
    public Class<?> getOutputKeyClass() {
      return LongWritable.class;
    }

    @Override
    public Class<?> getOutputValueClass() {
      return LongWritable.class;
    }

    @Override
    public KijiDataRequest getDataRequest() {
      return KijiDataRequest.builder()
          .addColumns(ColumnsDef.create()
              .withMaxVersions(HConstants.ALL_VERSIONS)
              .add("info", "address"))
          .build();
    }

    @Override
    public void gather(KijiRowData row, GathererContext<LongWritable, LongWritable> context)
        throws IOException {
      final Map<Long, CharSequence> addresses = row.<CharSequence>getValues("info", "address");
      context.write(new LongWritable(addresses.size()), new LongWritable(1));
    }
  }

  // -----------------------------------------------------------------------------------------------

  /** Aggregates the number of users for each number of addresses. */
  public static class Reducer
      extends KijiReducer<LongWritable, LongWritable, LongWritable, LongWritable> {

    /** {@inheritDoc} */
    @Override
    protected void reduce(LongWritable naddresses, Iterable<LongWritable> users, Context context)
        throws IOException, InterruptedException {
      long totalUsers = 0;
      for (LongWritable someUsers : users) {
        totalUsers += someUsers.get();
      }
      context.write(naddresses,  new LongWritable(totalUsers));
    }

    /** {@inheritDoc} */
    @Override
    public Class<?> getOutputKeyClass() {
      return LongWritable.class;
    }

    /** {@inheritDoc} */
    @Override
    public Class<?> getOutputValueClass() {
      return LongWritable.class;
    }
  }

  // -----------------------------------------------------------------------------------------------

  @Test
  public void run() throws Exception {
    final KijiURI tableURI = KijiURI.newBuilder(mKijiURI).withTableName("users").build();

    final Path output =
        new Path(String.format("file:///tmp/demo-table-mr-output-%d", System.currentTimeMillis()));

    final KijiMapReduceJob job = KijiGatherJobBuilder.create()
        .withConf(getConf())
        .withGatherer(Gatherer.class)
        .withReducer(Reducer.class)
        .withInputTable(tableURI)
        .withOutput(new SequenceFileMapReduceJobOutput(output, 16))
        .build();
    assertTrue(job.run());
  }
}
