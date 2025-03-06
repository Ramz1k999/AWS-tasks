package com.task11.utilities;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.HashMap;
import java.util.Map;

public class DynamoDBService {

    private final String tablesTableName;
    private final String reservationsTableName;
    private final DynamoDbClient dynamoDbClient;

    public DynamoDBService() {
        // Load environment variables
        this.tablesTableName = System.getenv("TABLES_TABLE_NAME");
        this.reservationsTableName = System.getenv("RESERVATIONS_TABLE_NAME");

        if (tablesTableName == null || reservationsTableName == null) {
            throw new IllegalStateException("Missing environment variables for DynamoDB table names.");
        }

        this.dynamoDbClient = DynamoDbClient.create();
    }

    public Map<String, Object> getAllTables() {
        ScanRequest scanRequest = ScanRequest.builder()
                .tableName(tablesTableName)
                .build();

        ScanResponse response = dynamoDbClient.scan(scanRequest);
        return Map.of("statusCode", 200, "body", response.items().toString());
    }

    public Map<String, Object> createTable(Map<String, Object> request) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().n(request.get("id").toString()).build());
        item.put("number", AttributeValue.builder().n(request.get("number").toString()).build());
        item.put("places", AttributeValue.builder().n(request.get("places").toString()).build());
        item.put("isVip", AttributeValue.builder().bool((Boolean) request.get("isVip")).build());

        if (request.containsKey("minOrder")) {
            item.put("minOrder", AttributeValue.builder().n(request.get("minOrder").toString()).build());
        }

        PutItemRequest putItemRequest = PutItemRequest.builder()
                .tableName(tablesTableName)
                .item(item)
                .build();

        dynamoDbClient.putItem(putItemRequest);
        return Map.of("statusCode", 200, "body", "Table created");
    }

    public Map<String, Object> getTableById(Map<String, Object> request) {
        GetItemRequest getItemRequest = GetItemRequest.builder()
                .tableName(tablesTableName)
                .key(Map.of("id", AttributeValue.builder().n(request.get("id").toString()).build()))
                .build();

        GetItemResponse response = dynamoDbClient.getItem(getItemRequest);
        return response.hasItem()
                ? Map.of("statusCode", 200, "body", response.item().toString())
                : Map.of("statusCode", 404, "body", "Table not found");
    }

    public Map<String, Object> getAllReservations() {
        ScanRequest scanRequest = ScanRequest.builder()
                .tableName(reservationsTableName)
                .build();

        ScanResponse response = dynamoDbClient.scan(scanRequest);
        return Map.of("statusCode", 200, "body", response.items().toString());
    }

    public Map<String, Object> createReservation(Map<String, Object> request) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("reservationId", AttributeValue.builder().s(request.get("reservationId").toString()).build());
        item.put("tableNumber", AttributeValue.builder().n(request.get("tableNumber").toString()).build());
        item.put("clientName", AttributeValue.builder().s(request.get("clientName").toString()).build());
        item.put("phoneNumber", AttributeValue.builder().s(request.get("phoneNumber").toString()).build());
        item.put("date", AttributeValue.builder().s(request.get("date").toString()).build());
        item.put("slotTimeStart", AttributeValue.builder().s(request.get("slotTimeStart").toString()).build());
        item.put("slotTimeEnd", AttributeValue.builder().s(request.get("slotTimeEnd").toString()).build());

        PutItemRequest putItemRequest = PutItemRequest.builder()
                .tableName(reservationsTableName)
                .item(item)
                .build();

        dynamoDbClient.putItem(putItemRequest);
        return Map.of("statusCode", 200, "body", "Reservation created");
    }
}
