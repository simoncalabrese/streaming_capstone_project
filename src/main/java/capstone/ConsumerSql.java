package capstone;

import capstone.bean.stream.BotBean;
import capstone.bean.stream.Interaction;
import capstone.cassandra.CassandraForEachWriter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.Function;

import java.math.BigDecimal;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.spark.sql.functions.*;

public class ConsumerSql {
    private final static StructField[] interactionFields = new StructField[]{
            new StructField("unix_time", DataTypes.LongType, true, Metadata.empty()),
            new StructField("category_id", DataTypes.LongType, true, Metadata.empty()),
            new StructField("ip", DataTypes.StringType, true, Metadata.empty()),
            new StructField("interactionType", DataTypes.StringType, true, Metadata.empty())
    };
    public final static StructType schema = new StructType(interactionFields);

    private static final SparkConf conf = new SparkConf()
            .setMaster("local[3]")
            .setAppName("Capstone");
    private static final String checkPoint = "Data/checkpoint";
    private static final SparkSession sparkSession = SparkSession.builder().config(conf)
            .getOrCreate();
    private static final Function<Iterator<Interaction>, Stream<Interaction>> TO_STREAM = iter -> {
        Iterable<Interaction> iterable = () -> iter;
        return StreamSupport.stream(iterable.spliterator(), false);
    };


    public static void main(String[] args) throws InterruptedException {


        final JavaStreamingContext ssc = JavaStreamingContext.getOrCreate(checkPoint, () -> {
            StreamingContext scont = new StreamingContext(sparkSession.sparkContext(), Durations.seconds(30));
            final JavaStreamingContext javaStreamingContext = new JavaStreamingContext(scont);
            Dataset<Row> ds1 = sparkSession
                    .readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
                    .option("subscribe", "eventbot")
                    .option("startingOffsets", "latest")
                    .option("failOnDataLoss", "false")
                    //.option("kafkaConsumer.pollTimeoutMs","10")
                    .load();
            final Dataset<Row> ds3 = ds1
                    .select(from_json(col("value").cast("string"), schema).as("interaction"));
            ds3.printSchema();
            final Dataset<Interaction> interaction = ds3.select(
                    col("interaction.unix_time").divide(lit(1000)).cast("timestamp").as("date").as(Encoders.LONG()),
                    col("interaction.category_id").as("categoryId").as(Encoders.LONG()),
                    col("interaction.ip").as(Encoders.STRING()),
                    col("interaction.interactionType").as(Encoders.STRING())
            ).as(Encoders.bean(Interaction.class));
            final Dataset<BotBean> bots = detect(interaction);
            final StreamingQuery start = bots.coalesce(1)
                    .writeStream()
                    .outputMode(OutputMode.Append())
                    .foreach(new CassandraForEachWriter())
                    .start();
            start.awaitTermination();
            return javaStreamingContext;
        });

        ssc.start();
        ssc.awaitTermination();
    }

    private static Dataset<BotBean> detect(final Dataset<Interaction> interaction) {
        interaction.printSchema();
        final Dataset<BotBean> bots = interaction
                .withWatermark("date", "10 minutes")
                .withColumn("window", window(col("date"), "10 seconds"))
                .coalesce(1).groupByKey((MapFunction<Row, String>) (row -> row.getAs("ip")), Encoders.STRING())
                .mapGroups((MapGroupsFunction<String, Row, BotBean>) ConsumerSql::getBotBean, Encoders.bean(BotBean.class)).filter((BotBean botBean) -> botBean.getRatio() || botBean.getTotCategories() || botBean.getTotRequests());
//        final Dataset<Row> bots = interaction/*.withWatermark("date", "10 minutes").groupBy(window(col("date"), "60 seconds"), col("ip"))*/
//                .groupBy(col("ip"))
//                .agg(ratio, totRequest, totCategories, totCategoriesAmount, ratioAmount, totRequestAmount)
//                .filter(col("ip").isNotNull())
//                .where(col("ratio").equalTo(true)
//                        .or(col("totRequests").equalTo(true)
//                                .or(col("totCategories").equalTo(true))));
        bots.printSchema();
        return bots;
    }

    public static Dataset<BotBean> analyze(final Dataset<Interaction> interaction) {
        final Dataset<BotBean> bots = interaction
                .withColumn("window", window(col("date"), "10 seconds"))
                .coalesce(1).groupByKey((MapFunction<Row, String>) (row -> row.getAs("ip")), Encoders.STRING())
                .mapGroups((MapGroupsFunction<String, Row, BotBean>) (s, iterator) -> getBotBean(s, iterator), Encoders.bean(BotBean.class)).filter((BotBean botBean) -> botBean.getRatio() || botBean.getTotCategories() || botBean.getTotRequests());
        return bots;
    }

    @NotNull
    private static BotBean getBotBean(String s, Iterator<Row> iterator) {
        final List<Row> interactionList = new ArrayList<>();
        iterator.forEachRemaining(interactionList::add);

        final Long totRequestLong = interactionList.stream().count();
        final Boolean totRequestBoolean = totRequestLong > 1000L;

        final Long totCategoriesLong = interactionList.stream().map(row -> row.getAs("categoryId")).distinct().count();
        final Boolean totCategoriesBoolean = totCategoriesLong > 5L;


        final Map<String, Long> clickView = interactionList.stream().map(row -> (String) row.getAs("interactionType"))
                .filter(Objects::nonNull).collect(Collectors.groupingBy(e -> e, Collectors.counting()));
        final Long click = clickView.getOrDefault("click", 0L);
        final Long view = clickView.getOrDefault("view", 0L);
        final Double ratioDouble;
        final Boolean ratioBoolean;
        if (view != 0) {
            ratioDouble = new BigDecimal(click)
                    .divide(new BigDecimal(view), 2, BigDecimal.ROUND_FLOOR).doubleValue();
            ratioBoolean = ratioDouble > 3D;
        } else {
            ratioDouble = 0D;
            ratioBoolean = false;
        }
        return new BotBean(s, ratioBoolean, totRequestBoolean, totCategoriesBoolean, ratioDouble, totRequestLong, totCategoriesLong);
    }


}
