package demo;

import org.apache.hadoop.hbase.HConstants;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

public class TestReadRow extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestReadRow.class);
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
                  .withFamily("browsing_history")
                      .withQualifier("http://www.wikipedia.com")
                          .withValue("<html>foo...</html>")
                      .withQualifier("http://www.google.com")
                          .withValue("<html>bar...</html>")
          .build();
    } else {
      mKijiURI = KijiURI.newBuilder("kiji://localhost:2181/default").build();
    }
  }

  // -----------------------------------------------------------------------------------------------

  @Test
  public void run() throws Exception {
    final Kiji kiji = Kiji.Factory.open(mKijiURI);
    try {
      final KijiTable table = kiji.openTable("users");
      try {
        final KijiTableReader reader = table.openTableReader();
        try {
          final EntityId entityId = table.getEntityId("mwallace");
          final KijiDataRequest dataRequest = KijiDataRequest.builder()
              .addColumns(ColumnsDef.create()
                  .withMaxVersions(1)  // 1 is the default: get the most recent cell!
                  .addFamily("info"))
              .addColumns(ColumnsDef.create()
                  .withMaxVersions(HConstants.ALL_VERSIONS)
                  .addFamily("browsing_history"))
              .build();
          final KijiRowData row = reader.get(entityId, dataRequest);
          LOG.info("Entity ID: {}", row.getEntityId());
          LOG.info("User full name: {}", row.getMostRecentCell("info", "full_name"));
          for (KijiCell<String> entry: row.<String>asIterable("browsing_history")) {
            LOG.info("Browsing history entry: {}", entry);
          }
        } finally {
          reader.close();
        }
      } finally {
        table.release();
      }
    }finally {
      kiji.release();
    }
  }
}
