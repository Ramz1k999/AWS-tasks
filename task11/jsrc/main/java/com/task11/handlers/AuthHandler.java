package com.task11.handlers;

import com.amazonaws.services.lambda.runtime.Context;
import com.task11.utilities.CognitoService;
import com.task11.utilities.ResponseUtil;

import java.util.Map;

public class AuthHandler {
    private final CognitoService cognitoService = new CognitoService();

    public Map<String, Object> handleSignup(Map<String, Object> request, Context context) {
        try {
            Map<String, Object> result = cognitoService.signup(request);
            return ResponseUtil.createResponse(200, result);
        } catch (Exception e) {
            return ResponseUtil.createResponse(400, "Signup failed: " + e.getMessage());
        }
    }

    public Map<String, Object> handleSignin(Map<String, Object> request, Context context) {
        try {
            Map<String, Object> result = cognitoService.signin(request);
            return ResponseUtil.createResponse(200, result);
        } catch (Exception e) {
            return ResponseUtil.createResponse(400, "Signin failed: " + e.getMessage());
        }
    }

