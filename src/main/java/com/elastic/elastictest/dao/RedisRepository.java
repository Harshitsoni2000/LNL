package com.elastic.elastictest.dao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class RedisRepository {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    public static final String KEY = "users";

    public Boolean add(String email, String data) {
        redisTemplate.opsForHash().put(KEY, ""+email, data);
        if(redisTemplate.opsForHash().get(KEY, email) != null)
            return true;
        return false;
    }

    public Object find(String email) {
        return redisTemplate.opsForHash().get(KEY, email);
    }
}
