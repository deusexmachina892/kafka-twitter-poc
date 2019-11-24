package org.echodigital.kafkajava;

import java.util.Properties;

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
public class ProducerDemoWithCallback {
    public static void main( String[] args ) {

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

        for ( int i = 0; i < 10; i++) {
                 // create producer record
            ProducerRecord<String, String> record = new ProducerRecord<String,String>("first_topic", "Hello World" + Integer.toString(i));
            producer.send(record, callback);
        }
      

        // flush data
        producer.flush();

        // flush + close
        producer.close();

    }
}
