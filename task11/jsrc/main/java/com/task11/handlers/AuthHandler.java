package com.task11.handlers;

import com.amazonaws.services.lambda.runtime.Context;
import com.task11.utilities.CognitoService;
import com.task11.utilities.ResponseUtil;

import java.util.Map;

public class AuthHandler {
    private final CognitoService cognitoService = new CognitoService();

    public Map<String, Object> handleSignup(Map<String, Object> request, Context context) {
        return cognitoService.signup(request);
    }

    public Map<String, Object> handleSignin(Map<String, Object> request, Context context) {
        return cognitoService.signin(request);
    }
}