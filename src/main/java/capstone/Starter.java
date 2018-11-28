package capstone;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class Starter {
    public static void main(String[] args) throws IOException {
//Create producer properties
        final Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //crete producer
        final KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        Files.readAllLines(Paths.get("BotGen/data.json")).forEach( r -> {
            final ProducerRecord<String, String> record = new ProducerRecord<>("stream-topics", r);
            kafkaProducer.send(record);
        });

        //flush data
        kafkaProducer.flush();

        //flush and close
        kafkaProducer.close();
    }
}
