package com.novus.authentication_service.configuration;

import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class DateConfiguration {

    public Date newDate() {
        return new Date(System.currentTimeMillis() + 3600000);
    }

}
