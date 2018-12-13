package capstone;

public class Starter {
    //    public static void main(String[] args) throws IOException {
////Create producer properties
//        final Properties properties = new Properties();
//        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
//        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        //crete producer
//        final KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
//        Files.readAllLines(Paths.get("BotGen/data.json")).forEach(r -> {
//            final ProducerRecord<String, String> record = new ProducerRecord<>("capstone-test", r);
//            kafkaProducer.send(record);
//        });
//
//        //flush data
//        kafkaProducer.flush();
//
//        //flush and close
//        kafkaProducer.close();
//    }
//    public static void main(String[] args) throws IOException {
//        test();
//    }
//
//    public static void test() throws IOException {
//        final Gson gson = new Gson();
//        Files.lines(Paths.get("../capstone-project/BotGen/data.json")).filter(Objects::nonNull).map(row -> {
//            final String json;
//            if (row.endsWith(",")) {
//                json = row.substring(0, row.length() - 1);
//            } else {
//                json = row;
//            }
//            return gson.fromJson(json, InteractionTest.class);
//        }).collect(Collectors.groupingBy(InteractionTest::getIp, Collectors.mapping(InteractionTest::getCategory_id, Collectors.toSet())))
//                .entrySet()
//                .stream()
//                .filter(e -> e.getValue().stream().distinct().count() > 5)
//                .forEach(System.out::println);
//    }
}
