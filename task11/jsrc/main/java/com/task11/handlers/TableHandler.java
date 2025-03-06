package com.task11.handlers;

import com.amazonaws.services.lambda.runtime.Context;
import com.task11.utilities.DynamoDBService;
import java.util.Map;

public class TableHandler {
    private final DynamoDBService dynamoDBService = new DynamoDBService();

    public Map<String, Object> getTables(Map<String, Object> request, Context context) {
        return dynamoDBService.getAllTables();
    }

    public Map<String, Object> createTable(Map<String, Object> request, Context context) {
        return dynamoDBService.createTable(request);
    }

    public Map<String, Object> getTableById(Map<String, Object> request, Context context) {
        return dynamoDBService.getTableById(request);
    }
}