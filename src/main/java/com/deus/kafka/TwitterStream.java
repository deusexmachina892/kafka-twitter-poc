package com.deus.kafka;

import com.deus.kafka.producers.TwitterProducer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * TwitterStream
 */
@SpringBootApplication
public class TwitterStream {

    public static void main(String[] args) {

        SpringApplication.run(TwitterStream.class, args);

        TwitterProducer producer = new TwitterProducer();
        producer.run();
    };
}