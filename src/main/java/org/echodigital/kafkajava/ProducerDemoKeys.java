package org.echodigital.kafkajava;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public class ProducerDemoKeys {
    public static void main(String[] args) throws InterruptedException, ExecutionException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        String bootstrapServers = "127.0.0.1:9092";
        
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // send data -asynchronous
        Callback callback = new Callback(){

            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                // executes everytime a record is sent or an exception is thrown
                if (exception != null) {
                    // record was successfully sent
                    logger.error("Error while producing", exception);

                } else {
                    logger.info("Received new message . \n" +
                        "Topic: " + metadata.topic() + "\n" + 
                        "Partion: " + metadata.partition() + "\n" + 
                        "Offset: " + metadata.offset() + "\n" + 
                        "Timestamp: " + metadata.timestamp());  
                }
                
            }
        };
        String topic = "first_topic";

        for ( int i = 0; i < 10; i++) {
            String value = "Hello World" + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            logger.info("Key: ", key);

            // create producer record
            ProducerRecord<String, String> record = new ProducerRecord<String,String>(topic, key, value);
            producer.send(record, callback).get(); // block the send to make it synchronous - don't do it in prodcution
        }
      

        // flush data
        producer.flush();

        // flush + close
        producer.close();

    }
}
