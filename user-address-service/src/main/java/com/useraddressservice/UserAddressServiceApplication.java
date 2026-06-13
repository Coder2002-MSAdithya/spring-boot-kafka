package com.useraddressservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class UserAddressServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(UserAddressServiceApplication.class, args);
    }

}
