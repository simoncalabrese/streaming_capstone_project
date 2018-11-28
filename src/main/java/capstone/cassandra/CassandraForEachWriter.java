package capstone.cassandra;

import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraForEachWriter extends ForeachWriter<Row> {
    private static final long serialVersionUID = 6593255972516907576L;
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraForEachWriter.class);

    private static final String IP = "ip";
    private static final String RATIO = "ratio";
    private static final String TOT_REQUESTS = "totRequests";
    private static final String TOT_CATEGORIES = "totCategories";
    private static final String REGISTER_DATE = "register_date";

    private static final CassandraConnector client = new CassandraConnector();

    static {
        client.connect("127.0.0.1", 9042);
    }

    @Override
    public boolean open(long partitionId, long version) {
        LOGGER.debug("Opening partition " + partitionId + " with version " + version);
        return true;
    }

    @Override
    public void process(Row value) {
        final String ip = value.getAs(IP);
        final Boolean ratio = value.getAs(RATIO);
        final Boolean totRequests = value.getAs(TOT_REQUESTS);
        final Boolean totCategories = value.getAs(TOT_CATEGORIES);
        final Long registerDate = System.currentTimeMillis();

        String query = "INSERT INTO " +
                "capstone.bots" + "(" + IP + "," + RATIO + "," + TOT_REQUESTS + "," + TOT_CATEGORIES + "," + REGISTER_DATE + ") " +
                "VALUES ('" + ip + "', " + ratio + ", " + totRequests + ", " + totCategories + "," + registerDate + ");";
        LOGGER.info("Performing: " + query);
        client.getSession().execute(query);
    }

    @Override
    public void close(Throwable errorOrNull) {
        LOGGER.debug("Closing...");
    }
}
