package capstone.ignite;

import capstone.bean.stream.Interaction;
import capstone.cassandra.CassandraConnector;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

public class IgniteWriter extends ForeachWriter<Interaction> {
    private static final Logger LOGGER = LoggerFactory.getLogger(IgniteWriter.class);
    private static final long serialVersionUID = 5751129873954580682L;
    public static final IgniteDAO ignite = null;//new IgniteDAO();

    @Override
    public boolean open(long partitionId, long version) {
        LOGGER.info("partitionId:" + partitionId + "version:" + version);
        return true;
    }

    @Override
    public void process(Interaction value) {
        ignite.addInteraction(value);
    }

    @Override
    public void close(Throwable errorOrNull) {
        LOGGER.info("Closed");
    }
}
