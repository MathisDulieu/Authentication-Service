package com.novus.authentication_service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@RequiredArgsConstructor
public class UuidProvider {

    public String generateUuid() { return UUID.randomUUID().toString(); }

}