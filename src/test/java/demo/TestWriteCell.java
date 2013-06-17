package demo;

import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

public class TestWriteCell extends KijiClientTest {
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

  @Test
  public void run() throws Exception {
    final Kiji kiji = Kiji.Factory.open(mKijiURI);
    try {
      final KijiTable table = kiji.openTable("users");
      try {
        final KijiTableWriter writer = table.openTableWriter();
        try {
          final EntityId entityId = table.getEntityId("mwallace");
          final String url = "http://www.wikipedia.com";
          writer.put(entityId, "browsing_history", url, "<html>...</html>");
        } finally {
          writer.close();
        }
      } finally {
        table.release();
      }
    }finally {
      kiji.release();
    }
  }
}
