package demo;

import org.apache.hadoop.hbase.HConstants;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableReader.KijiScannerOptions;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

public class TestScanRow extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestScanRow.class);
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
              .withRow("vvega")
                  .withFamily("info")
                      .withQualifier("full_name")
                          .withValue("Vincent Vega")
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
          final KijiDataRequest dataRequest = KijiDataRequest.builder()
              .addColumns(ColumnsDef.create()
                  .withMaxVersions(1)  // 1 is the default: get the most recent cell!
                  .addFamily("info"))
              .addColumns(ColumnsDef.create()
                  .withMaxVersions(HConstants.ALL_VERSIONS)
                  .addFamily("browsing_history"))
              .build();
          final KijiScannerOptions scannerOptions = new KijiScannerOptions();
              // .setStartRow(startRow)
              // .setStopRow(stopRow)

          final KijiRowScanner scanner = reader.getScanner(dataRequest, scannerOptions);
          try {
            for (final KijiRowData row : scanner) {
              LOG.info("==========");
              LOG.info("Entity ID: {}", row.getEntityId());
              LOG.info("User full name: {}", row.getMostRecentCell("info", "full_name"));
              for (KijiCell<String> entry: row.<String>asIterable("browsing_history")) {
                LOG.info("Browsing history entry: {}", entry);
              }
            }
          } finally {
            scanner.close();
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
