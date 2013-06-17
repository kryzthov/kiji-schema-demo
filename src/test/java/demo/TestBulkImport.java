package demo;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.TestingResources;

import org.kiji.mapreduce.KijiMapReduceJob;
import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.bulkimport.KijiBulkImportJobBuilder;
import org.kiji.mapreduce.bulkimport.KijiBulkImporter;
import org.kiji.mapreduce.input.MapReduceJobInputs;
import org.kiji.mapreduce.output.DirectKijiTableMapReduceJobOutput;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

public class TestBulkImport extends KijiClientTest {
  private KijiURI mKijiURI;

  private static final boolean FAKE = true;

  @Before
  public void setup() throws Exception {
    if (FAKE) {
      mKijiURI = getKiji().getURI();
      new InstanceBuilder(getKiji())
          .withTable(KijiTableLayouts.getLayout("users.json"))
          .build();
    } else {
      mKijiURI = KijiURI.newBuilder("kiji://localhost:2181/default").build();
    }
  }

  // -----------------------------------------------------------------------------------------------

  public static final class Importer extends KijiBulkImporter<LongWritable, Text> {
    private static final Logger LOG = LoggerFactory.getLogger(Importer.class);

    public static enum Counters { BAD_INPUT_LINE }

    @Override
    public void produce(LongWritable pos, Text text, KijiTableContext context) throws IOException {
      final String line = text.toString();
      final String[] split = line.split("\t", 3);
      if (split.length != 3) {
        LOG.error("Ignoring input line: {}", line);
        context.incrementCounter(Counters.BAD_INPUT_LINE);
        return;
      }
      final String login = split[0];
      final String url = split[1];
      final String html = split[2];

      final EntityId entityId = context.getEntityId(login);
      context.put(entityId,  "browsing_history", url, html);
    }
  }

  // -----------------------------------------------------------------------------------------------

  @Test
  public void run() throws Exception {
    // Prepare input file:
    final File inputFile = File.createTempFile("TestBulkImportInput", ".txt", getLocalTempDir());
    TestingResources.writeTextFile(inputFile,
        TestingResources.get("bulk-import-data.txt"));

    // Run the bulk-import:
    final KijiMapReduceJob job = KijiBulkImportJobBuilder.create()
        .withConf(getConf())
        .withBulkImporter(Importer.class)
        .withInput(MapReduceJobInputs.newTextMapReduceJobInput(new Path(inputFile.toString())))
        .withOutput(new DirectKijiTableMapReduceJobOutput(
            KijiURI.newBuilder(mKijiURI).withTableName("users").build()))
        .build();
    assertTrue(job.run());
  }
}
