package iudx.connector.ngsild;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import io.vertx.core.MultiMap;
import io.vertx.core.json.JsonObject;

public class QueryMapper {

	public JsonObject getIUDXQuery(MultiMap paramsMap) {
		JsonObject rootNode = new JsonObject();
		paramsMap.forEach(entry -> {
			(rootNode).put(NGSI2IUDXMapping.valueOf(entry.getKey()).getValue(),
					this.mapperDataTypeHelper(entry.getKey(), entry));
		});

		if (paramsMap.contains("timerel")) {
			if (paramsMap.get("timerel").equalsIgnoreCase("between")) {
				rootNode.put("TRelation", "during");
				rootNode.put("time",
						paramsMap.get("time") + "/" + paramsMap.get("endtime"));
			}
		}
		else {
			rootNode.put("options", "latest");
		}

		if (paramsMap.contains("georel")) {
			String relation = paramsMap.get("georel");
			if (relation.contains(";")) {
				String rel[] = relation.split(";");
				rootNode.put("relation", rel[0]);
				String dist = rel[1].substring(rel[1].indexOf("=") + 1);
				rootNode.put("distance", dist);
			}
			else {
				rootNode.put("relation", relation);
			}
		}

		System.out.println(rootNode);
		return rootNode;
	}

	private Object mapperDataTypeHelper(String key, Map.Entry<String, String> entry) {
		if (key.equalsIgnoreCase("id") || key.equalsIgnoreCase("attrs")) {
			return entry.getValue();
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
	type("resource-server-id"),
	coordinates("coordinates"),
	geometry("geometry"),
	timerel("trelation"),
	georel("relation"),
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

enum GeoRelationMapping {
	near("near"),
	within("within"),
	contains("contains"),
	overlaps("overlaps"),
	intersect("intersect"),
	equal("equal"),
	disjoint("disjoint");

	private final String value;

	GeoRelationMapping(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}
}
