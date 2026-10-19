package com.pavan.ingestion.exception;

public class EventIdConflictException extends RuntimeException {

    public EventIdConflictException(String message) {
        super(message);
    }
}
