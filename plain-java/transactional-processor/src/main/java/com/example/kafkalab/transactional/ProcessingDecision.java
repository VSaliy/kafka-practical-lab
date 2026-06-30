package com.example.kafkalab.transactional;

record ProcessingDecision(String topic, String key, String value) {
}
