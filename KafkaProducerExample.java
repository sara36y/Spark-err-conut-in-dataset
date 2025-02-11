package com.example;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class KafkaProducerExample {
    public static void main(String[] args) throws IOException {
        String filePath = "C:\\kafka\\kafka_2.12-3.9.0\\DataFormed.csv";  // الملف اللي هتقرأ منه البيانات
        String bootstrapServers = "localhost:9092";  // مكان Kafka
        String topic = "logs-topic";  // الموضوع اللي هننشر فيه البيانات

        // إعداد خصائص Kafka Producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // إنشاء Kafka Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // قراءة البيانات من CSV
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                // نشر كل سجل إلى Kafka
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, line);
                producer.send(record);
            }
        }

        producer.close();  // غلق الاتصال بعد إرسال البيانات
    }
}
