package com.datorama.timbermill;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ElasticsearchClientMock extends ElasticsearchClient {

    private Map<String, Task> indexedTasks = new HashMap<>();

    public ElasticsearchClientMock() {
        super(null, "", 0, null, 0, 0);
    }


    @Override
    public Map<String, Task> fetchIndexedTasks(Set<String> eventsToFetch) {
        return eventsToFetch.stream()
                .filter(indexedTasks::containsKey)
                .collect(Collectors.toMap(Function.identity(), indexedTasks::get));
    }

    @Override
    public void indexTasks(Map<String, Task> tasksToIndex) {
        indexedTasks.putAll(tasksToIndex);
    }


    @Override
    public void indexTaskToMetaDataIndex(Task task) {
        indexedTasks.put(task.getTaskId(), task);
    }

	public Task getTaskById(String taskId){
		return indexedTasks.get(taskId);
	}
}
