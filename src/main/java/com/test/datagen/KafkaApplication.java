package com.test.datagen;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@SpringBootApplication
public class KafkaApplication {

	private final static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

	public static void main(String[] args) throws Exception {

		ConfigurableApplicationContext context = SpringApplication.run(KafkaApplication.class, args);
		MessageProducer producer = context.getBean(MessageProducer.class);
		producer.beepForAnHour();

		context.close();
	}

	@Bean
	public MessageProducer messageProducer() {
		return new MessageProducer();
	}

	public static class MessageProducer {

		@Autowired
		private KafkaTemplate<String, String> kafkaTemplate;

		@Value(value = "${kafka.topic}")
		private String topicName;

		public void beepForAnHour() {
			final Runnable beeper = new Runnable() {
				public void run() {
					System.out.println("beep");
					record();
				}
			};
			final ScheduledFuture<?> beeperHandle = scheduler.scheduleAtFixedRate(beeper, 10, 10, SECONDS);
			scheduler.schedule(new Runnable() {
				public void run() {
					beeperHandle.cancel(true);
				}
			}, 60 * 60, SECONDS);
		}

		public void record() {
			String testMessage ="He Kapil Jain ! This side";
			ObjectMapper Obj = new ObjectMapper();
			try {
				String jsonStr = Obj.writeValueAsString(testMessage);
				sendMessage(jsonStr);
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
		}

		public void sendMessage(final String message) {
			ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, message);
			future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

				@Override
				public void onSuccess(SendResult<String, String> result) {
					System.out.println(
							"Sent message=[" + message + "] with offset=[" + result.getRecordMetadata().offset() + "]");
				}

				@Override
				public void onFailure(Throwable ex) {
					System.out.println("Unable to send message=[" + message + "] due to : " + ex.getMessage());
				}
			});
		}
	}
}
