package iudx.connector.ngsild;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.vertx.core.MultiMap;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class QueryMapper {
	
	
	public static void main(String[] args) {
		MultiMap map = MultiMap.caseInsensitiveMultiMap();
		map.add("id",
				"rbccps.org/aa9d66a000d94a78895de8d4c0b3a67f3450e531/pudx-resource-server/pune-itms/pune-itms-live");
		map.add("timerel", "between");
		map.add("time", "2020-01-23T14:20:00Z");
		map.add("endtime", "2020-01-23T14:40:00Z");
		map.add("georel", "near;maxDistance==360");
		map.add("geometry", "point");
		map.add("coordinates","%5B8.684783577919006%2C49.406131991436396%5D");
		//map.add("coordinates","%5B%5B%5B8.684628009796143%2C49.406062179606515%5D%2C%5B8.685507774353027%2C49.4062262372493%5D%2C%5B8.68545413017273%2C49.40634491690448%5D%2C%5B8.684579730033875%2C49.40617736907259%5D%2C%5B8.684628009796143%2C49.406062179606515%5D%5D%5D");
		map.add("q", "LIGHT>2900");
		map.add("attrs", "CURRENT_STATUS,ROUTE_ID");
		QueryMapper qm = new QueryMapper();
		//qm.getIUDXQuery(map);
		System.out.println(qm.getIUDXQuery(map));

	}

	public JsonObject getIUDXQuery(MultiMap paramsMap) {
		JsonObject rootNode = new JsonObject();
		paramsMap.forEach(entry -> {
			if (entry.getKey().equals("q"))
				return;
			rootNode.put(NGSI2IUDXMapping.valueOf(entry.getKey()).getValue(),
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

		if (paramsMap.contains("q")) {
			String qFilter = paramsMap.get("q");
			System.out.println("qFilter " + qFilter);
			String[] options = qFilter.split(";");
			//System.out.println("options " + options);
			Arrays.stream(options).forEach(e -> {
				System.out.println(e);
				List<String> queryTerms = getQueryTerms(e);
				rootNode.put("attribute-name", queryTerms.get(0));
				rootNode.put("attribute-value", queryTerms.get(2));
				rootNode.put("comparison-operator",
						QueryOperators.getName4Value(queryTerms.get(1)));
			});
		}

		if (paramsMap.contains("geometry") && paramsMap.contains("coordinates") && paramsMap.contains("georel")) {
			String coordinates = rootNode.getString("coordinates").replaceAll("\\[|\\]","");
			String geomType = paramsMap.get("geometry");
			String georel = paramsMap.get("georel");
			if (geomType.equalsIgnoreCase("polygon")) {
				String geom = geomType + "((" + coordinates + "))";
				rootNode.put("geometry", geom);
				rootNode.put("georel", georel);
			}
			else if (geomType.equalsIgnoreCase("LineString")
					|| geomType.equalsIgnoreCase("MultiLineString")) {
				String geom = geomType + "(" + coordinates + ")";
				rootNode.put("geometry", geom);
				rootNode.put("georel", georel);
			}
			else if (geomType.equalsIgnoreCase("point")) {
				//handle probable circle geom here.
				String radius = null;
				if(georel.contains("maxDistance") || georel.contains("maxdistance"))
					radius=georel.split(";")[1].split("==")[1];
				String[] lat_lon=coordinates.split(",");
				String lat=lat_lon[0];
				String lon=lat_lon[1];
				
				rootNode.put("radius", radius);
				rootNode.put("lon", lon);
				rootNode.put("lat", lat);
				
				rootNode.remove("relation");// remove relation node from final json
				rootNode.remove("geometry");// remove geometry node as it is not required in circle case.
			}
			rootNode.remove("coordinates");//remove coordinates from final json object -> translated to geometry;
			rootNode.remove("georel");//remove georel from final jsonobject;
		}
		return rootNode;
	}

	private List<String> getQueryTerms(String queryTerms) {
		List<String> qTerms = new ArrayList<String>();
		int length = queryTerms.length();
		int startIndex = 0;
		boolean specialCharFound = false;
		char[] allowedSpecialCharacter = ">=<!".toCharArray();
		for (int i = 0; i < length; i++) {
			Character c = queryTerms.charAt(i);

			if (!(Character.isLetter(c) || Character.isDigit(c)) && !specialCharFound) {
				for(int j=0;j<allowedSpecialCharacter.length;j++) {
					if (allowedSpecialCharacter[j] == c) {
						qTerms.add(queryTerms.substring(startIndex, i));
						startIndex = i;
						specialCharFound = true;
					} else {
						System.out.println("Ignore " + c.toString());				
					}
				}
			}
			else {
				if (specialCharFound && (Character.isLetter(c) || Character.isDigit(c))) {
					qTerms.add(queryTerms.substring(startIndex, i));
					qTerms.add(queryTerms.substring(i));
					break;
				}
			}

		}
		return qTerms;
	}

	private Object mapperDataTypeHelper(String key, Map.Entry<String, String> entry) {
		if (key.equalsIgnoreCase("id")) {
			return entry.getValue();
		}
		else if (key.equalsIgnoreCase("attrs")) {
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
	type("resource-server-id"),
	coordinates("coordinates"),
	geometry("geometry"),
	timerel("TRelation"),
	georel("relation"),
	endtime("time"),
	time("time"),
	q("q");

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
	intersects("intersects"),
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

enum QueryOperators {

	propertyisequalto("=="),
	propertyisnotequalto("!="),
	propertyisgreaterthan(">"),
	propertyislessthan("<"),
	propertyislessthanequalto("<="),
	propertyisgreaterthanequalto(">="),
	propertyislike("like");

	private final String value;

	QueryOperators(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}

	public static String getName4Value(String value) {
		System.out.println(value);
		for (QueryOperators element : QueryOperators.values()) {
			if (element.value.equalsIgnoreCase(value)) {
				return element.name();
			}
		}
		return null;
	}
}
