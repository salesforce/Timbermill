package com.datorama.timbermill;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;

public class PluginsConfig {
	private static final Logger LOG = LoggerFactory.getLogger(PluginsConfig.class);

	public static Collection<TaskLogPlugin> initPluginsFromJson(String pluginsJson) {
		GsonBuilder gsonBuilder = new GsonBuilder();

		gsonBuilder.registerTypeAdapter(TaskLogPlugin.class, deserializer);

		Gson customGson = gsonBuilder.create();

		Type listType = new TypeToken<ArrayList<TaskLogPlugin>>(){}.getType();
		Collection<TaskLogPlugin> ret = customGson.fromJson(pluginsJson, listType);

		int i = 1;
		for (TaskLogPlugin p : ret) {
			if (p.getName() == null) {
				p.setName(p.getClass().getSimpleName() + "_" + i);
			}
			i++;
		}

		LOG.info("Loaded " + ret.size() + " plugins: " + ret);
		LOG.info("Parsed json: " + new Gson().toJson(ret));

		return ret;
	}

	private static JsonDeserializer<TaskLogPlugin> deserializer = (json, typeOfT, context) -> {
		JsonObject jsonObject = json.getAsJsonObject();
		JsonPrimitive prim = (JsonPrimitive) jsonObject.get("class");
		String className = prim.getAsString();

		if (className.indexOf('.') == -1) {
			className = "com.datorama.microservice.timbermill.plugins." + className;
		}

		Class<?> klass;
		try {
			klass = Class.forName(className);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new JsonParseException(e.getMessage());
		}

		return context.deserialize(jsonObject, klass);
	};
}
