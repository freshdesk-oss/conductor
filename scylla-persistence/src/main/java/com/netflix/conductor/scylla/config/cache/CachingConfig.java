/*
 * Copyright 2022 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.netflix.conductor.scylla.config.cache;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableCaching
public class CachingConfig {
    public static final String TASK_DEF_CACHE = "taskDefCache";
    public static final String EVENT_HANDLER_CACHE = "eventHandlerCache";
    public static final String SHARD_ID_CACHE = "shardIdCache";

    @Bean
    public CacheManager cacheManager() {
        // Old cache manager for backward compatibility (TASK_DEF_CACHE, EVENT_HANDLER_CACHE)
        ConcurrentMapCacheManager oldCacheManager = new ConcurrentMapCacheManager(TASK_DEF_CACHE, EVENT_HANDLER_CACHE);

        // Caffeine cache manager for new cache (SHARD_ID_CACHE) with TTL and LRU
        CaffeineCacheManager caffeineCacheManager = new CaffeineCacheManager(SHARD_ID_CACHE);
        caffeineCacheManager.setCaffeine(Caffeine.newBuilder()
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .maximumSize(1000000)
        );

        // Composite CacheManager to handle both old and new caches
        return new CacheManager() {
            @Override
            public org.springframework.cache.Cache getCache(String name) {
                if (SHARD_ID_CACHE.equals(name)) {
                    return caffeineCacheManager.getCache(name);
                }
                return oldCacheManager.getCache(name);
            }

            @Override
            public Collection<String> getCacheNames() {
                // Return the cache names for both old and new caches
                return Arrays.asList(TASK_DEF_CACHE, EVENT_HANDLER_CACHE, SHARD_ID_CACHE);
            }
        };
    }
}
