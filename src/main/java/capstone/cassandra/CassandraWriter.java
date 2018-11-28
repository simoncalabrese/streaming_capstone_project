package capstone.cassandra;

import capstone.bean.ignite.BotBean;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

public class CassandraWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraWriter.class);

    private static final String IP = "ip";
    private static final String REGISTER_DATE = "register_date";

    private static final CassandraConnector client = new CassandraConnector();

    static {
        client.connect("127.0.0.1", 9042);
    }

    public static void process(BotBean value) {
        final String ip = value.getIp();
        final Long registerDate = value.getRegisterDate();

        String query = "INSERT INTO " +
                "capstone.bots" + "(" + IP + "," + REGISTER_DATE + ") " +
                "VALUES ('" + ip + "', " + registerDate + ");";
        LOGGER.info("Performing: " + query);
        client.getSession().execute(query);
    }
}
