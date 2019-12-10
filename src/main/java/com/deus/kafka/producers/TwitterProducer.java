package com.deus.kafka.producers;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * TwitterProducer
 */

public class TwitterProducer {

   Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

   final String CONSUMER_KEY = "o7AXKpzsUjjz8wghaaIsq43WB";
   final String CONSUMER_SECRET = "SWeE7FXyfYoBzLsUeQ4BnBYt2hpQPaVimfcl3Gby0C4KxRZcUu";
   final String TOKEN = "4332649467-6Yw6XNOCBU3HyPwnQMlyG6zJwKlFRxm3DRNQdgT";
   final String SECRET = "ezxwpwJJYXBk8eZnbiahXCPg3DKKxSIExNBFxGcEAmcXQ";
  
    // Optional: set up some followings and track terms
    List<String> terms = Lists.newArrayList("kafka");

    public TwitterProducer(){};

    public void run() {
        // create a twitter client
        /**
         * Set up your blocking queues: Be sure to size these properly based on expected
         * TPS of your stream
         */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        // Create a Twitter Client
        Client client = createTwitterClient(msgQueue);

        // Attempts to establish a connection.
        client.connect();

        // create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()-> {
            logger.info("Shutting down...");
            logger.info("Closing Twitter client...");
            client.stop();
            logger.info("Shutting down producer...");
            producer.close();
            logger.info("Done!");
        }));

        // loop to send tweets to kafka

        while (!client.isDone()) {
            String msg = null;

            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                client.stop();
            };

            if(msg != null) {
                logger.info(msg);
                ProducerRecord<String, String> record = new ProducerRecord<String,String>("twitter_tweets", null, msg);
                Callback callback = new Callback(){
                
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if(exception != null) {
                            logger.error("Something bad happened!", exception);
                        };
                    };
                };

                producer.send(record, callback);
            };
        };
        
        logger.info("End of application");
    };

    public Client createTwitterClient(BlockingQueue<String> msgQueue){

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
   
        hosebirdEndpoint.trackTerms(terms);

        logger.info(Constants.STREAM_HOST);
        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, SECRET);

        ClientBuilder builder = new ClientBuilder();

        builder
            .name("Hosebird-Client-01")                              // optional: mainly for the logs
            .hosts(hosebirdHosts)
            .authentication(hosebirdAuth)
            .endpoint(hosebirdEndpoint)
            .processor(new StringDelimitedProcessor(msgQueue));
            //.eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        
        return hosebirdClient;
};

    public KafkaProducer<String, String> createKafkaProducer(){
        
        final String bootstrapServers = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        return producer;
    };

}