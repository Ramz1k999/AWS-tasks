package com.task11.handlers;

import com.amazonaws.services.lambda.runtime.Context;
import com.task11.utilities.*;

import java.util.Map;

public class TableHandler {
    private final DynamoDBService dynamoDBService = new DynamoDBService();

    public Map<String, Object> getTables(Map<String, Object> request, Context context) {
        try {
            Map<String, Object> tables = dynamoDBService.getAllTables();
            return ResponseUtil.createResponse(200, tables);
        } catch (Exception e) {
            return ResponseUtil.createResponse(400, "Error fetching tables: " + e.getMessage());
        }
    }

    public Map<String, Object> createTable(Map<String, Object> request, Context context) {
        try {
            Map<String, Object> result = dynamoDBService.createTable(request);
            return ResponseUtil.createResponse(200, result);
        } catch (Exception e) {
            return ResponseUtil.createResponse(400, "Error creating table: " + e.getMessage());
        }
    }

    public Map<String, Object> getTableById(Map<String, Object> request, Context context) {
        try {
            Map<String, Object> table = dynamoDBService.getTableById(request);
            return ResponseUtil.createResponse(200, table);
        } catch (Exception e) {
            return ResponseUtil.createResponse(400, "Error fetching table details: " + e.getMessage());
        }
    }
}