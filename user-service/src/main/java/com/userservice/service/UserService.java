package com.userservice.service;

import com.userservice.dto.request.UserCreateRequest;
import com.userservice.dto.response.AddressResponseDto;
import com.userservice.dto.response.UserResponse;
import com.userservice.entity.User;
import com.userservice.producer.KafkaProducer;
import com.userservice.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Optional;

@Service
@RequiredArgsConstructor
public class UserService {

    private final UserRepository userRepository;
    private final KafkaProducer kafkaProducer;

    public User create(final UserCreateRequest userCreateRequest) {
        final User user = getUser(userCreateRequest);
        final User savedUser = userRepository.save(user);
        kafkaProducer.publishUserRegistration(savedUser, userCreateRequest.getAddressText());
        return savedUser;
    }

    public UserResponse getUserAddress(final Long userId) {
        final String url = String.format("http://localhost:8802/api/address/%s", userId);
        final RestTemplate restTemplate = new RestTemplate();
        final ResponseEntity<AddressResponseDto> address = restTemplate.getForEntity(url, AddressResponseDto.class);
        final User user = getUserById(address.getBody().getUserId());
        return UserResponse.getUserResponseWithAddress(user, address.getBody());
    }

    private User getUserById(final Long id) {
        final Optional<User> userOptional = userRepository.findById(id);
        return userOptional.orElse(null);
    }

    private User getUser(final UserCreateRequest userCreateRequest) {
        return User.builder()
                .firstName(userCreateRequest.getFirstName())
                .lastName(userCreateRequest.getLastName())
                .email(userCreateRequest.getEmail())
                .build();
    }
}
