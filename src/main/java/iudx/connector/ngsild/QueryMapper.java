package iudx.connector.ngsild;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.Identity;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.vertx.core.MultiMap;
import io.vertx.core.json.JsonObject;

public class QueryMapper {

	public JsonObject getIUDXQuery(MultiMap paramsMap) {
		JsonObject rootNode = new JsonObject();
		paramsMap.forEach(entry -> {
			if(entry.getKey().equals("q")) return;
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
		
		if(paramsMap.contains("q")) {
			String qFilter=paramsMap.get("q");
			//rootNode= handleAttrFilterQuery(qFilter,rootNode);
			String[] options=qFilter.split(";");
			Arrays.stream(options).forEach(e->{
				List<String> queryTerms=getQueryTerms(e);
				rootNode.put("attribute-name", queryTerms.get(0));
				rootNode.put("comparison-operator", QueryOperators.getName4Value( queryTerms.get(1)));
				rootNode.put("attribute-value", queryTerms.get(2));
			});
		}
		
		System.out.println(rootNode);
		return rootNode;
	}
	
	
	public static void main(String[] args) {
		//System.out.println(QuerySpecialCharacter.getName4Value("=="));
		/*
		 * QueryMapper qm=new QueryMapper(); List<String>
		 * s=qm.getQueryTerms("temprature>=30"); s.forEach(System.out::println);
		 */
		
		
		MultiMap map=MultiMap.caseInsensitiveMultiMap();
		map.add("id", "abc");
		map.add("georel","near;maxDistance=2000");
		map.add("geometry","polygon");
		map.add("coordinates", "%5B%5B%5B8.684628009796143%2C49.406062179606515%5D%2C%5B8.685507774353027%2C49.4062262372493%5D%2C%5B8.68545413017273%2C49.40634491690448%5D%2C%5B8.684579730033875%2C49.40617736907259%5D%2C%5B8.684628009796143%2C49.406062179606515%5D%5D%5D");
		map.add("q","temprature>=20;");
		QueryMapper qm=new QueryMapper();
		qm.getIUDXQuery(map);
		
	}
	
	private List<String> getQueryTerms(String queryTerms) {
		List<String> qTerms=new ArrayList<String>();
		int length=queryTerms.length();
		int startIndex=0;
		boolean specialCharFound=false;
		for(int i=0;i<length;i++) {
			Character c=queryTerms.charAt(i);
			if(!(Character.isLetter(c) || Character.isDigit(c)) && !specialCharFound) {
				qTerms.add(queryTerms.substring(startIndex, i));
				startIndex=i;
				specialCharFound=true;
				
			}else {
				if(specialCharFound && (Character.isLetter(c) || Character.isDigit(c))) {
					qTerms.add(queryTerms.substring(startIndex, i));
					qTerms.add(queryTerms.substring(i));
					break;
				}
			}
			
		}
		return qTerms;
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
		/*
		 * else if(key.equalsIgnoreCase("q")) { return null; }
		 */
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

enum QueryOperators{
	
	PropertyIsEqualTo("=="),
	PropertyIsNotEqualTo("!="),
	PropertyIsGreaterThan(">"),
	PropertyIsLessThan("<"),
	PropertyIsLesThanOrEqualTo("<="),
	PropertyIsGreaterThanOrEqualTo(">="),
	PropertyIsLike("like");
	
	
	private final String value;

	QueryOperators(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}
	
	
	public static String getName4Value(String value) {
		for(QueryOperators element:QueryOperators.values()) {
			if(element.value.equalsIgnoreCase(value)) {
				return element.name();
			}
		}
		return null;
	}
}
