package com.datorama.timbermill.unit;

import java.time.ZonedDateTime;
import java.util.Map;

import com.datorama.timbermill.common.TimbermillUtils;

import static com.datorama.timbermill.common.Constants.EXCEPTION;

public class PluginApplierTask extends Task{
    public PluginApplierTask(String env, String pluginName, String pluginClass, TaskStatus status, String exception, ZonedDateTime endTime, long duration, ZonedDateTime startTime, long daysRotation) {
        setName("metadata_timbermill_plugin");
        setEnv(env);
        setStartTime(startTime);
        setEndTime(endTime);
        setDuration(duration);
        setDateToDelete(TimbermillUtils.getDefaultDateToDelete(daysRotation));
        setStatus(status);

        Map<String, String> tasksStrings = getString();
        tasksStrings.put("pluginName", pluginName);
        tasksStrings.put("pluginClass", pluginClass);
        if (exception != null){
            getText().put(EXCEPTION, exception);
        }
    }
}
