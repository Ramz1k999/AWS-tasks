package com.task11.handlers;

import com.amazonaws.services.lambda.runtime.Context;
import com.task11.utilities.*;

import java.util.Map;

public class ReservationHandler {
    private final DynamoDBService dynamoDBService = new DynamoDBService();

    public Map<String, Object> getReservations(Map<String, Object> request, Context context) {
        try {
            Map<String, Object> reservations = dynamoDBService.getAllReservations();
            return ResponseUtil.createResponse(200, reservations);
        } catch (Exception e) {
            return ResponseUtil.createResponse(400, "Error fetching reservations: " + e.getMessage());
        }
    }

    public Map<String, Object> createReservation(Map<String, Object> request, Context context) {
        try {
            Map<String, Object> result = dynamoDBService.createReservation(request);
            return ResponseUtil.createResponse(200, result);
        } catch (Exception e) {
            return ResponseUtil.createResponse(400, "Error creating reservation: " + e.getMessage());
        }
    }
}