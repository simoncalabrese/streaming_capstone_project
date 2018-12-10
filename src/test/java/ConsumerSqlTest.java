import capstone.ConsumerSql;
import capstone.bean.stream.Interaction;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.unix_timestamp;

public class ConsumerSqlTest {


    private Tuple2<SparkSession, JavaSparkContext> getSparkSession() {
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Capstone");
        final String checkPoint = "Data/checkpoint";
        final SparkSession sparkSession = SparkSession.builder().config(conf)
                .getOrCreate();
        final JavaSparkContext javacontext = new JavaSparkContext(sparkSession.sparkContext());
        return Tuple2.apply(sparkSession, javacontext);
    }


    @Test
    public void noBot() {
        List<InteractionTest> interactions = Arrays.asList(
                new InteractionTest("2018-01-12 12:10:00", 1000L, "172.15.0.0", "view"),
                new InteractionTest("2018-01-12 12:15:00", 1001L, "172.10.0.0", "view"),
                new InteractionTest("2018-01-12 12:15:00", 1001L, "172.10.0.0", "view"),
                new InteractionTest("2018-01-12 12:15:00", 1002L, "172.20.0.0", "click"),
                new InteractionTest("2018-01-12 12:15:00", 1002L, "172.10.0.0", "click"),
                new InteractionTest("2018-01-12 12:10:00", 1003L, "172.20.0.0", "view"),
                new InteractionTest("2018-01-12 12:15:00", 1003L, "172.20.0.0", "click"));
        final Dataset<Row> bots = execute(interactions);
        Assert.assertEquals(bots.count(),0);
    }

    @Test
    public void botForTotCategories() {
        List<InteractionTest> interactions = Arrays.asList(
                new InteractionTest("2018-01-12 12:10:00", 1000L, "172.10.0.0", "view"),
                new InteractionTest("2018-01-12 12:15:00", 1001L, "172.10.0.0", "view"),
                new InteractionTest("2018-01-12 12:15:00", 1002L, "172.10.0.0", "view"),
                new InteractionTest("2018-01-12 12:15:00", 1003L, "172.10.0.0", "click"),
                new InteractionTest("2018-01-12 12:15:00", 1004L, "172.10.0.0", "click"),
                new InteractionTest("2018-01-12 12:10:00", 1005L, "172.20.0.0", "view"),
                new InteractionTest("2018-01-12 12:15:00", 1006L, "172.10.0.0", "click"));
        final Dataset<Row> bots = execute(interactions);
        final String ip = bots.select("ip").collectAsList().get(0).getString(0);
        Assert.assertEquals(ip,"172.10.0.0");
    }

    @Test
    public void botForClickViewRatio() {
        List<InteractionTest> interactions = Arrays.asList(new InteractionTest("2018-01-12 12:10:00", 1000L, "172.10.0.0", "view"),
                new InteractionTest("2018-01-12 12:10:00", 1000L, "172.10.0.0", "click"),
                new InteractionTest("2018-01-12 12:15:00", 1000L, "172.10.0.0", "click"),
                new InteractionTest("2018-01-12 12:15:00", 1000L, "172.10.0.0", "click"),
                new InteractionTest("2018-01-12 12:15:00", 1000L, "172.10.0.0", "click"),
                new InteractionTest("2018-01-12 12:10:00", 1000L, "172.20.0.0", "view"),
                new InteractionTest("2018-01-12 12:15:00", 1000L, "172.20.0.0", "click"));
        final Dataset<Row> bots = execute(interactions);
        final String ip = bots.select("ip").collectAsList().get(0).getString(0);
        Assert.assertEquals(ip, "172.10.0.0");
        Assert.assertEquals(bots.count(), 1);
    }

    @Test
    public void botForEventRateMoreThan1000() {
        final List<InteractionTest> requestsOfTheSame = IntStream.rangeClosed(0, 1000)
                .boxed().map(i -> new InteractionTest("2018-01-12 12:10:00", 1000L, "172.10.0.0", "view"))
                .collect(Collectors.toList());

        final List<InteractionTest> others = Arrays.asList(
                new InteractionTest("2018-01-12 12:20:00", 1001L, "172.10.0.0", "click"),
                new InteractionTest("2018-01-12 12:10:00", 1002L, "172.20.0.0", "view"),
                new InteractionTest("2018-01-12 12:20:00", 1003L, "172.20.0.0", "click"));
        requestsOfTheSame.addAll(others);
        final Dataset<Row> bots = execute(requestsOfTheSame);
        final String ip = bots.select("ip").collectAsList().get(0).getString(0);
        Assert.assertEquals(ip, "172.10.0.0");
        Assert.assertEquals(bots.count(), 1);
    }

    private Dataset<Row> execute(final List<InteractionTest> interactions) {
        final Tuple2<SparkSession, JavaSparkContext> sparkSession = getSparkSession();
        final Dataset<Interaction> test = sparkSession._1().createDataset(sparkSession._2().parallelize(interactions).rdd(), Encoders.bean(InteractionTest.class))
                .select(unix_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss").cast("timestamp").as(Encoders.LONG()).as("date"), col("categoryId").as(Encoders.LONG()),
                        col("ip").as(Encoders.STRING()), col("interactionType").as(Encoders.LONG()))
                .as(Encoders.bean(Interaction.class));
        return ConsumerSql.analyze(test);
    }

}