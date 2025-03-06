package com.task11.handlers;

import com.amazonaws.services.lambda.runtime.Context;
import com.task11.utilities.DynamoDBService;
import java.util.Map;

public class ReservationHandler {
    private final DynamoDBService dynamoDBService = new DynamoDBService();

    public Map<String, Object> getReservations(Map<String, Object> request, Context context) {
        return dynamoDBService.getAllReservations();
    }

    public Map<String, Object> createReservation(Map<String, Object> request, Context context) {
        return dynamoDBService.createReservation(request);
    }
}