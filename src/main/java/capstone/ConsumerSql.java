package capstone;

import capstone.bean.stream.Interaction;
import capstone.cassandra.CassandraForEachWriter;
import org.apache.spark.SparkConf;
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

import static org.apache.spark.sql.functions.*;

public class ConsumerSql {
    private final static StructField[] interactionFields = new StructField[]{
            new StructField("unix_time", DataTypes.LongType, true, Metadata.empty()),
            new StructField("category_id", DataTypes.LongType, true, Metadata.empty()),
            new StructField("ip", DataTypes.StringType, true, Metadata.empty()),
            new StructField("interactionType", DataTypes.StringType, true, Metadata.empty())
    };
    private final static StructType schema = new StructType(interactionFields);

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Capstone");
        final String checkPoint = "Data/checkpoint";
        final SparkSession sparkSession = SparkSession.builder().config(conf)
                .getOrCreate();

        final JavaStreamingContext ssc = JavaStreamingContext.getOrCreate(checkPoint, () -> {
            StreamingContext scont = new StreamingContext(sparkSession.sparkContext(), Durations.seconds(30));
            final JavaStreamingContext javaStreamingContext = new JavaStreamingContext(scont);
            Dataset<Row> ds1 = sparkSession
                    .readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
                    .option("subscribe", "stream-topics")
                    .option("startingOffsets", "latest")
                    .option("failOnDataLoss", "false")
                    //.option("kafkaConsumer.pollTimeoutMs","10")
                    .load();
            final PartialCollector partialCollector = new PartialCollector();
            final Dataset<Row> ds3 = ds1
                    .select(from_json(col("value").cast("string"), schema).as("interaction"));
            ds3.printSchema();
            final Dataset<Interaction> interaction = ds3.select(
                    col("interaction.unix_time").divide(lit(1000)).cast("timestamp").as("date").as(Encoders.LONG()),
                    col("interaction.category_id").as("categoryId").as(Encoders.LONG()),
                    col("interaction.ip").as(Encoders.STRING()),
                    col("interaction.interactionType").as(Encoders.STRING())
            ).as(Encoders.bean(Interaction.class));
            interaction.printSchema();
            final Column clickCol = count(
                    when(
                            col("interactionType").equalTo("click"), 0
                    )
            ).as("totClick");
            final Column viewCol = count(
                    when(
                            col("interactionType").equalTo("view"), 0
                    )
            ).as("totView");
            final Column ratioAmount = coalesce(clickCol.divide(viewCol),lit(0)).as("ratioAmount");
            final Column totRequestAmount = count(col("date")).as("totRequestsAmount");
            final Column ratio = ratioAmount.gt(3).as("ratio");
            final Column totRequest = count(col("date")).gt(lit(1000)).as("totRequests");
            final Column totCategoriesAmount = partialCollector.apply(col("categoryId")).as("totCategoriesAmount");
            final Column totCategories = totCategoriesAmount.gt(lit(20)).as("totCategories");
            final Dataset<Row> bots = interaction.groupBy(window(col("date"), "10 minutes"), col("ip"))
                    .agg(ratio, totRequest, totCategories, totCategoriesAmount, ratioAmount, totRequestAmount)
                    .filter(col("ip").isNotNull())
                    .where(col("ratio").equalTo(true)
                            .or(col("totRequests").equalTo(true)
                                    .or(col("totCategories").equalTo(true))));
            bots.printSchema();
            final StreamingQuery start = bots.coalesce(1)
                    .writeStream()
                    .outputMode(OutputMode.Complete())
                    .foreach(new CassandraForEachWriter())
                    .start();
            start.awaitTermination();
            return javaStreamingContext;
        });

        ssc.start();
        ssc.awaitTermination();
    }


}
