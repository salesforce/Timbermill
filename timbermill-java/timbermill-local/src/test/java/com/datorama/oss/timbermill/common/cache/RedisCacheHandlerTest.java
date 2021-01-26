package com.datorama.oss.timbermill.common.cache;

import com.datorama.oss.timbermill.unit.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class RedisCacheHandlerTest {

    private RedisCacheHandler redisCacheHandler;

    @Before
    public void setUp() {
        redisCacheHandler = new RedisCacheHandler(1000000, "localhost",
                6379, "", "1000000000", "volatile-lru", false, 10000, 10000);
    }

    @Test
    @Ignore
    public void getFromALotTasksCache() {
        Collection<String> list = Lists.newLinkedList();
        for (int i = 0; i < 1000000; i++) {
            list.add("a" + i);
        }
        Map<String, LocalTask> fromTasksCache = redisCacheHandler.getFromTasksCache(list);
        int i = 0;
    }

    @Test
    @Ignore
    public void pushALotToTasksCache() {
        Map<String, LocalTask> map = Maps.newHashMap();
        List<Event> events = Lists.newArrayList(new StartEvent("bla", "bla", LogParams.create(), null), new SuccessEvent("bla", LogParams.create()));
        LocalTask task = new LocalTask(new Task(events, 90, "bla"));
        for (int i = 0; i < 1000000; i++) {
            map.put("a" + i, task);
        }
        redisCacheHandler.pushToTasksCache(map);
    }
}