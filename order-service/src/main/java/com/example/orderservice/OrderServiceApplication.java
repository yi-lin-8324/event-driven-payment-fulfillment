package com.example.orderservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class OrderServiceApplication {
	static {
		System.setProperty("org.apache.avro.SERIALIZABLE_PACKAGES", "com.example.orderservice.avro");
	}

	public static void main(String[] args) {
		SpringApplication.run(OrderServiceApplication.class, args);
	}
}