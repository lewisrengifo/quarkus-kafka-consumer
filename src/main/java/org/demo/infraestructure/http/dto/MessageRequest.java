package org.demo.infraestructure.http.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MessageRequest {
    @JsonProperty("message")
    private String message;

    public MessageRequest(String message) {
        this.message = message;
    }

    // Getters and setters
    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
