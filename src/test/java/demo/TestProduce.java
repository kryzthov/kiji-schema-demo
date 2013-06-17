package demo;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.util.Utf8;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.KijiMapReduceJob;
import org.kiji.mapreduce.output.DirectKijiTableMapReduceJobOutput;
import org.kiji.mapreduce.produce.KijiProduceJobBuilder;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

public class TestProduce extends KijiClientTest {
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
                          .withValue("San Francisco, 94110 CA")
              .withRow("vvega")
                  .withFamily("info")
                      .withQualifier("full_name")
                          .withValue("Vincent Vega")
                      .withQualifier("address")
                          .withValue("San Francisco, 94131 CA")
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

  public static final class Producer extends KijiProducer {
    private static final Logger LOG = LoggerFactory.getLogger(TestProduce.class);
    private static final Pattern RE_ZIP_CODE = Pattern.compile("(\\d{5})");

    public static enum Counters { NO_ADDRESS, NO_ZIP_CODE }

    @Override
    public KijiDataRequest getDataRequest() {
      return KijiDataRequest.builder()
          .addColumns(ColumnsDef.create().addFamily("info"))
          .build();
    }

    @Override
    public String getOutputColumn() {
      return "info:zip_code";
    }

    @Override
    public void produce(KijiRowData row, ProducerContext context) throws IOException {
      final Utf8 address = row.<Utf8>getMostRecentValue("info", "address");
      if (address == null) {
        LOG.info("No address for {}", row.getEntityId());
        context.incrementCounter(Counters.NO_ADDRESS);
        return;
      }
      final Matcher matcher = RE_ZIP_CODE.matcher(address);
      if (matcher.find()) {
        final int zipCode = Integer.parseInt(matcher.group(0));
        context.put(zipCode);
      } else {
        LOG.info("No zip-code in address for {}", row.getEntityId());
        context.incrementCounter(Counters.NO_ZIP_CODE);
      }
    }
  }

  // -----------------------------------------------------------------------------------------------

  @Test
  public void run() throws Exception {
    final KijiURI tableURI = KijiURI.newBuilder(mKijiURI).withTableName("users").build();

    // Run the producer:
    final KijiMapReduceJob job = KijiProduceJobBuilder.create()
        .withConf(getConf())
        .withProducer(Producer.class)
        .withInputTable(tableURI)
        .withOutput(new DirectKijiTableMapReduceJobOutput(tableURI))
        .build();
    assertTrue(job.run());
  }
}
