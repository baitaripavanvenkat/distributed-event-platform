package com.pavan.ingestion.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(EventIdConflictException.class)
    public ResponseEntity<Map<String, String>> handleEventIdConflict(
            EventIdConflictException exception) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(Map.of(
                        "error", "EVENT_ID_CONFLICT",
                        "message", exception.getMessage()
                ));
    }
}
