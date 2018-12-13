package capstone.cassandra;

import capstone.bean.stream.BotBean;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class CassandraForEachWriter extends ForeachWriter<BotBean> {
    private static final long serialVersionUID = 6593255972516907576L;
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraForEachWriter.class);

    private static final String IP = "ip";
    private static final String RATIO = "ratio";
    private static final String TOT_REQUESTS = "totRequests";
    private static final String TOT_CATEGORIES = "totCategories";
    private static final String RATIO_AMNT = "ratioAmount";
    private static final String TOT_REQUESTS_AMNT = "totRequestsAmount";
    private static final String TOT_CATEGORIES_AMNT = "totCategoriesAmount";
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
    public void process(BotBean value) {
        final String ip = value.getIp();
        final Boolean ratio = value.getRatio();
        final Boolean totRequests = value.getTotRequests();
        final Boolean totCategories = value.getTotCategories();
        final Double ratioAmount = value.getRatioAmount();
        final Long totRequestsAmount = value.getTotRequestsAmont();
        final Long totCategoriesAmount = value.getTotCategoriesAmount();
        final Long registerDate = System.currentTimeMillis();

        String query = "INSERT INTO " +
                "capstone.bots" + "(" + IP + "," + RATIO + "," + TOT_REQUESTS + "," + TOT_CATEGORIES + "," + REGISTER_DATE + "," + RATIO_AMNT + "," + TOT_REQUESTS_AMNT + "," + TOT_CATEGORIES_AMNT + ") " +
                "VALUES ('" + ip + "', " + ratio + ", " + totRequests + ", " + totCategories + "," + registerDate + "," + ratioAmount + "," + totRequestsAmount + ", " + totCategoriesAmount + ");";
        LOGGER.info("Performing: " + query);
        client.getSession().execute(query);
    }

    @Override
    public void close(Throwable errorOrNull) {
        LOGGER.debug("Closing...");
    }
}
