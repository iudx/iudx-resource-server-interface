package iudx.connector.ngsild;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.vertx.core.MultiMap;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class QueryMapper {

	public JsonObject getIUDXQuery(MultiMap paramsMap) {
		JsonObject rootNode = new JsonObject();
		paramsMap.forEach(entry -> {
			(rootNode).put(NGSI2IUDXMapping.valueOf(entry.getKey()).getValue(),
					this.mapperDataTypeHelper(entry.getKey(), entry));
		});

		if (paramsMap.contains("timerel")
				&& paramsMap.get("timerel").equalsIgnoreCase("between")) {
			rootNode.put("trelation", "during");
			rootNode.put("time", paramsMap.get("time") + "/" + paramsMap.get("endtime"));

		}
		System.out.println(rootNode);
		return rootNode;
	}

	public static void main(String[] args) {

		MultiMap map = MultiMap.caseInsensitiveMultiMap();
		map.add("id", "123");
		map.add("attrs", "temprature,astavs,asyst");
		map.add("type", "Vehicle");
		map.add("coordinates",
				"%5B%5B%5B8.684628009796143%2C49.406062179606515%5D%2C%5B8.685507774353027%2C49.4062262372493%5D%2C%5B8.68545413017273%2C49.40634491690448%5D%2C%5B8.684579730033875%2C49.40617736907259%5D%2C%5B8.684628009796143%2C49.406062179606515%5D%5D%5D");
		map.add("geometry", "polygon");
		map.add("timerel", "after");
		map.add("time", "2017-07-29T12:00:04");
		map.add("endtime", "2017-07-30T12:00:04");

		QueryMapper qm = new QueryMapper();
		qm.getIUDXQuery(map);
	}

	private Object mapperDataTypeHelper(String key, Map.Entry<String, String> entry) {
		if (key.equalsIgnoreCase("id") || key.equalsIgnoreCase("attrs")) {
			JsonArray array = new JsonArray();
			List<String> list = Arrays.stream(entry.getValue().split(","))
					.collect(Collectors.toList());
			list.forEach(s -> array.add(s));
			return array;
		}
		else if (key.equalsIgnoreCase("geometry")) {
			return entry.getValue();
		}
		else if (key.equalsIgnoreCase("coordinates")) {
			try {
				return URLDecoder.decode(entry.getValue(),
						StandardCharsets.UTF_8.toString());
			}
			catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
			return "";
		}
		else if (key.equals("timerel")) {
			return entry.getValue().toString();
		}
		else {
			return entry.getValue();
		}
	}

}

enum NGSI2IUDXMapping {
	id("id"), 
	attrs("attribute-filter"), 
	type("resource-server-id"), // TODO : discuss whether it is correct or not.
	coordinates("coordinates"), 
	geometry("geometry"), 
	timerel("trelation"), 
	endtime("time"), 
	time("time");

	private final String value;

	NGSI2IUDXMapping(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}

}
