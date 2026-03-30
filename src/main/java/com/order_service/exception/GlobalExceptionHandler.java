package com.order_service.exception;


import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import com.order_service.controller.response.ErrorResponse;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ServerWebExchange;

import java.time.LocalDateTime;


@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Object> handleBadRequest(IllegalArgumentException ex, ServerWebExchange exchange) {
        return buildErrorResponse(HttpStatus.BAD_REQUEST, ex.getMessage(), exchange.getRequest().getPath().value());
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Object> handleValidation(MethodArgumentNotValidException ex, ServerWebExchange exchange) {
        return buildErrorResponse(HttpStatus.BAD_REQUEST, ex.getMessage(), exchange.getRequest().getPath().value());
    }

    @ExceptionHandler(DataIntegrityViolationException.class)
    public ResponseEntity<Object> handleConstraintViolation(DataIntegrityViolationException ex, ServerWebExchange exchange) {
        return buildErrorResponse(HttpStatus.BAD_REQUEST, ex.getMessage(), exchange.getRequest().getPath().value());
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Object> handleAll(Exception ex, ServerWebExchange exchange) {
        return buildErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage(), exchange.getRequest().getPath().value());
    }

    private ResponseEntity<Object> buildErrorResponse(HttpStatus status, String message, String path) {
        ErrorResponse body = new ErrorResponse(LocalDateTime.now(), status.value(), message, path);
        return ResponseEntity.status(status).body(body);
    }
}