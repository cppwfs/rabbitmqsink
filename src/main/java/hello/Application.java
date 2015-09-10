/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hello;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class Application implements CommandLineRunner {

	final static String queueName = "spring-boot";

	@Autowired
	AnnotationConfigApplicationContext context;

	@Bean
	Queue queue() {
		return new Queue(queueName, false);
	}

	@Bean
	TopicExchange exchange() {
		return new TopicExchange("spring-boot-exchange");
	}

	@Bean
	Binding binding(Queue queue, TopicExchange exchange) {
		return BindingBuilder.bind(queue).to(exchange).with(queueName);
	}

	@Bean
	SimpleMessageListenerContainer container(ConnectionFactory connectionFactory, MessageListenerAdapter listenerAdapter) {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
		container.setConnectionFactory(connectionFactory);
		container.setQueueNames(queueName);
		container.setMessageListener(listenerAdapter);
		container.setPrefetchCount(800);
		return container;
	}

	@Bean
	Receiver receiver() {
		return new Receiver();
	}

	@Bean
	MessageListenerAdapter listenerAdapter(Application receiver) {
		return new MessageListenerAdapter(receiver, "receiveMessage");
	}

	public static void main(String[] args) throws InterruptedException {
		SpringApplication.run(Application.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		System.out.println("Preparing to receive messages...");
	}
	public void receiveMessage(String foo) {
		if (start.get() == -1L) {
			synchronized (start) {
				if (start.get() == -1L) {
					start();
					// assume a homogeneous message structure - this is intended for perf tests so we can safely assume
					// that the messages are similar,  Therefore we'll do our reporting based on the first message
					start.set(clock.now());
					executorService.execute(new ReportStats());
					System.out.println("Ready");
				}
			}
		}
		intermediateCounter.incrementAndGet();
		if (reportBytes) {
			intermediateBytes.addAndGet(((foo).getBytes()).length);
		}

	}
	private static Logger logger = LoggerFactory.getLogger(Application.class);


	private final AtomicLong counter = new AtomicLong();

	private final AtomicLong start = new AtomicLong(-1);

	private final AtomicLong bytes = new AtomicLong(-1);

	private final AtomicLong intermediateCounter = new AtomicLong();

	private final AtomicLong intermediateBytes = new AtomicLong();

	private TimeUnit timeUnit = TimeUnit.s;

	private final Clock clock = new Clock();

	private volatile boolean running;

	private ExecutorService executorService;

	private boolean reportBytes = false;

	public void start() {
		if (executorService == null) {
			executorService = Executors.newFixedThreadPool(1);
		}
		this.running = true;
	}


	public void stop() {
		this.running = false;
	}

	public boolean isRunning() {
		return running;
	}

	public static class Clock {
		public long now() {
			return System.currentTimeMillis();
		}
	}
	private class ReportStats implements Runnable {

		@Override
		public void run() {
			int reportEveryMs = 1000;
			while (isRunning()) {
				long intervalStart = clock.now();
				try {
					Thread.sleep(reportEveryMs);
					long timeNow = clock.now();
					long currentCounter = intermediateCounter.getAndSet(0L);
					long currentBytes = intermediateBytes.getAndSet(0L);
					long totalCounter = counter.addAndGet(currentCounter);
					long totalBytes = bytes.addAndGet(currentBytes);

					logger.info(
							String.format("Messages: %10d in %5.2f%s = %11.2f/s",
									currentCounter,
									(timeNow - intervalStart)/ 1000.0, timeUnit, ((double) currentCounter * 1000 / reportEveryMs)));
					logger.info(
							String.format("Messages: %10d in %5.2f%s = %11.2f/s",
									totalCounter, (timeNow - start.get()) / 1000.0, timeUnit,
									((double) totalCounter * 1000 / (timeNow - start.get()))));
					if (reportBytes) {
						logger.info(
								String.format("Throughput: %12d in %5.2f%s = %11.2fMB/s, ",
										currentBytes,
										(timeNow - intervalStart)/ 1000.0, timeUnit,
										((currentBytes / (1024.0 * 1024)) * 1000 / reportEveryMs)));
						logger.info(
								String.format("Throughput: %12d in %5.2f%s = %11.2fMB/s",
										totalBytes, (timeNow - start.get()) / 1000.0, timeUnit,
										((totalBytes / (1024.0 * 1024)) * 1000 / (timeNow - start.get()))));
					}
				}
				catch (InterruptedException e) {
					if (!isRunning()) {
						Thread.currentThread().interrupt();
					}
				}
			}
		}
	}
}