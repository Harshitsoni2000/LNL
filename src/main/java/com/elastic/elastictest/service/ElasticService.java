package com.elastic.elastictest.service;

import com.elastic.elastictest.dao.RedisRepository;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Service
public class ElasticService {

    @Autowired
    private RedisRepository repository;

    public String getDataFromElastic(String field, String text) {

        if(StringUtils.isEmpty(text.trim()))
            return "";


//        GET CATEGORIES, SUBCATEGORIES, RESPONSES separately.
//        HttpEntity<String> getCategoryEntity = new HttpEntity<>(findCategoryQuery, headers);
//        HttpEntity<String> getSubCategoryEntity = new HttpEntity<>(findSubCategoryQuery, headers);
//        HttpEntity<String> getResponseEntity = new HttpEntity<>(findResponseQuery, headers);

//        ResponseEntity<String> categoryResult = template.exchange(url, HttpMethod.POST, getCategoryEntity, String.class);
//        JSONArray categoryResultArray = new JSONObject(categoryResult.getBody()).getJSONObject("aggregations").getJSONObject("category_agg").getJSONArray("buckets");
//        ResponseEntity<String> subcategoryResult = template.exchange(url, HttpMethod.POST, getSubCategoryEntity, String.class);
//        JSONArray subCategoryResultArray = new JSONObject(subcategoryResult.getBody()).getJSONObject("aggregations").getJSONObject("subcategory_agg").getJSONArray("buckets");
//        ResponseEntity<String> responseResult = template.exchange(url, HttpMethod.POST, getResponseEntity, String.class);
//        JSONArray responseResultArray = new JSONObject(responseResult.getBody()).getJSONObject("aggregations").getJSONObject("response_agg").getJSONArray("buckets");

//        String getAllDetailsQuery =
//                "{}\n"+
//                "{\"size\": 0, \"query\": {\"wildcard\": {\"category_name\": {\"value\": \"*"+text+"*\"}}}, \"aggs\": {\"categories\": {\"terms\": {\"field\": \"category_name.keyword\",\"size\": 5, \"order\": {\"_term\": \"asc\"}}}}}\n"+
//                "{}\n"+
//                "{\"size\": 0, \"query\": {\"wildcard\": {\"sub_category_name\": {\"value\": \"*"+text+"*\"}}}, \"aggs\": {\"subcategories\": {\"terms\": {\"field\": \"sub_category_name.keyword\",\"size\": 5, \"order\": {\"_term\": \"asc\"}}}}}\n"+
//                "{}\n"+
//                "{\"size\": 0, \"query\": {\"wildcard\": {\"response_name\": {\"value\": \"*"+text+"*\"}}}, \"aggs\": {\"responses\": {\"terms\": {\"field\": \"response_name.keyword\",\"size\": 5, \"order\": {\"_term\": \"asc\"}}}}}\n"+
//                "{}\n"+
//                "{\"size\": 0, \"query\": {\"wildcard\": {\"response_body\": {\"value\": \"*"+text+"*\"}}}, \"aggs\": {\"previews\": {\"terms\": {\"field\": \"response_body.keyword\",\"size\": 5, \"order\": {\"_term\": \"asc\"}}}}}\n";

//        String getAllDetailsQuery =
//                        "{}\n"+
//                        "{\"size\": 5, \"query\": {\"multi_match\": {\"query\": \""+text+"\", \"fuzziness\": \"2\",\"fields\": [\"category_name\"], \"minimum_should_match\": \"80%\"}},\"sort\": [{\"category_name.keyword\": {\"order\": \"desc\"}}]}\n"+
//                        "{}\n"+
//                        "{\"size\": 5, \"query\": {\"multi_match\": {\"query\": \""+text+"\", \"fuzziness\": \"2\",\"fields\": [\"sub_category_name\"], \"minimum_should_match\": \"80%\"}},\"sort\": [{\"sub_category_name.keyword\": {\"order\": \"desc\"}}]}\n"+
//                        "{}\n"+
//                        "{\"size\": 5, \"query\": {\"multi_match\": {\"query\": \""+text+"\", \"fuzziness\": \"2\",\"fields\": [\"response_name\"], \"minimum_should_match\": \"80%\"}},\"sort\": [{\"response_name.keyword\": {\"order\": \"desc\"}}]}\n"+
//                        "{}\n"+
//                        "{\"size\": 5, \"query\": {\"multi_match\": {\"query\": \""+text+"\", \"fuzziness\": \"2\",\"fields\": [\"response_body\"], \"minimum_should_match\": \"80%\"}},\"sort\": [{\"response_body.keyword\": {\"order\": \"desc\"}}]}\n";

        String query =
                        "{ "+
                        "\"size\": 10, "+
                        "\"query\": { "+
                        "\"multi_match\": { "+
                        "\"query\": \""+text+"\", "+
                        "\"fields\": [\""+field+"\"], "+
                        "\"fuzziness\": 1, "+
                        "\"minimum_should_match\": \"100%\" "+
                        "} "+
                        "}, "+
                        "\"sort\": [ "+
                        "{ "+
                        "\"price\": { "+
                        "\"order\": \"asc\" "+
                        "} "+
                        "} "+
                        "] "+
                        "}";

//        String url = "http://172.16.1.113:9200/canned_response/_msearch";
        String url = "http://localhost:9200/airbnb/_search";
        return getResponseEntity(query, url).getJSONObject("hits").getJSONArray("hits").toString();
//        JSONArray responseArray = responseObject.getJSONArray("responses");
//        JSONObject jsonObject = new JSONObject();

//        String queryAgain = "";
//        List<String> types = new ArrayList<>(Arrays.asList("categories", "subcategories", "responses", "previews"));
//        return responseObject.getJSONObject("hits").getJSONArray("hits").toString();
//        JSONArray result = responseArray.getJSONObject(0).getJSONObject("hits").getJSONArray("hits");
//        JSONArray result = responseArray.getJSONObject(0).getJSONObject("aggregations").getJSONObject("categories").getJSONArray("buckets");
//        if(result.isEmpty()) {
//            queryAgain +=
//                    "{}\n"+"{\"size\":0,\"query\":{\"multi_match\": {\"query\": \""+text+"\",\"fields\": [\"category_name\"], \"minimum_should_match\": \"100%\", \"fuzziness\": 1}}, \"aggs\": {\"categories\": {\"terms\": {\"field\": \"category_name.keyword\",\"size\": 5, \"order\": {\"_term\": \"asc\"}}}}}\n";
//        } else
//            jsonObject.put("categories", result);
//
//        result = responseArray.getJSONObject(1).getJSONObject("aggregations").getJSONObject("subcategories").getJSONArray("buckets");
//        if(result.isEmpty()) {
//            queryAgain +=
//                    "{}\n"+"{\"size\":0,\"query\":{\"multi_match\": {\"query\": \""+text+"\",\"fields\": [\"sub_category_name\"], \"minimum_should_match\": \"100%\", \"fuzziness\": 1}}, \"aggs\": {\"subcategories\": {\"terms\": {\"field\": \"sub_category_name.keyword\",\"size\": 5, \"order\": {\"_term\": \"asc\"}}}}}\n";
//        } else
//            jsonObject.put("subcategories", result);
//
//        result = responseArray.getJSONObject(2).getJSONObject("aggregations").getJSONObject("responses").getJSONArray("buckets");
//        if(result.isEmpty()) {
//            queryAgain +=
//                    "{}\n"+"{\"size\":0,\"query\":{\"multi_match\": {\"query\": \""+text+"\",\"fields\": [\"response_name\"], \"minimum_should_match\": \"100%\", \"fuzziness\": 1}}, \"aggs\": {\"responses\": {\"terms\": {\"field\": \"response_name.keyword\",\"size\": 5, \"order\": {\"_term\": \"asc\"}}}}}\n";
//        } else
//            jsonObject.put("responses", result);
//
//        result = responseArray.getJSONObject(3).getJSONObject("aggregations").getJSONObject("previews").getJSONArray("buckets");
//        if(result.isEmpty()) {
//            queryAgain +=
//                    "{}\n"+"{\"size\":0,\"query\":{\"multi_match\": {\"query\": \""+text+"\",\"fields\": [\"response_body\"], \"minimum_should_match\": \"100%\", \"fuzziness\": 1}}, \"aggs\": {\"previews\": {\"terms\": {\"field\": \"response_body.keyword\",\"size\": 5, \"order\": {\"_term\": \"asc\"}}}}}\n";
//        } else
//            jsonObject.put("previews", result);
//
//        if(queryAgain.isEmpty())
//            return jsonObject.toString();
//
//        resultEntity = new HttpEntity<>(queryAgain, headers);
//        responseEntity = template.exchange(url, HttpMethod.POST, resultEntity, String.class);
//        responseObject = new JSONObject(responseEntity.getBody());
//        responseArray = responseObject.getJSONArray("responses");
//
//        int index = 0;
//        if(!jsonObject.has("categories"))
//            jsonObject.put("categories", responseArray.getJSONObject(0).getJSONObject("aggregations").getJSONObject("categories").getJSONArray("buckets"));
//        if(!jsonObject.has("subcategories"))
//            jsonObject.put("subcategories", responseArray.getJSONObject(1).getJSONObject("aggregations").getJSONObject("subcategories").getJSONArray("buckets"));
//        if(!jsonObject.has("responses"))
//            jsonObject.put("responses", responseArray.getJSONObject(2).getJSONObject("aggregations").getJSONObject("responses").getJSONArray("buckets"));
//        if(!jsonObject.has("previews"))
//            jsonObject.put("previews", responseArray.getJSONObject(3).getJSONObject("aggregations").getJSONObject("previews").getJSONArray("buckets"));
//
//        return jsonObject.toString();

    }

    public String getDataFromElastic(String id) {
        String query =
                "{ "+
                        "\"query\": { "+
                        "\"term\": { "+
                        "\"_id\": { "+
                        "\"value\": \""+id+"\" "+
                        "} "+
                        "} "+
                        "} "+
                        "}";

        return callApi(query, "airbnb").get(0).toString();
    }

    public String getDataFromElastic(List<String> listIds) {
        String start = "\"", end = "\n";
        String formatted = start;
        for(String id : listIds)
            formatted += id+"\",\"";
        String result = formatted.substring(0, formatted.length()-2)+end;
        String query =
                "{\n" +
                        "  \"query\": {\n" +
                        "    \"bool\": {\n" +
                        "      \"should\": [\n" +
                        "        {\n" +
                        "          \"terms\": {\n" +
                        "            \"_id\": [\n" +
                        result +
                        "            ]\n" +
                        "          }\n" +
                        "        }\n" +
                        "      ]\n" +
                        "    }\n" +
                        "  },\n" +
                        "  \"sort\": [\n" +
                        "    {\n" +
                        "      \"review_scores_rating\": {\n" +
                        "        \"order\": \"desc\"\n" +
                        "      }\n" +
                        "    }\n" +
                        "  ]\n" +
                        "}";

        return callApi(query, "airbnb").toString();
    }

    public String getReviews(String id) {
        String query =
                "{\n" +
                        "  \"size\": 30, \n" +
                        "  \"query\": {\n" +
                        "    \"bool\": {\n" +
                        "      \"must\": [\n" +
                        "        {\n" +
                        "          \"match\": {\n" +
                        "            \"listing_id\": \""+id+"\"\n" +
                        "          }\n" +
                        "        }\n" +
                        "      ]\n" +
                        "    }\n" +
                        "  }\n" +
                        "}";

        return callApi(query, "airbnb_reviews").toString();
    }

    public String signIn(String data) {
        JSONObject jsonObject = new JSONObject(data);
        JSONObject returnMessage = new JSONObject();
        String userId = jsonObject.getString("email");
        if(jsonObject.getBoolean("isHost")) {
            String userData = getPropertiesByHostId(userId);
            if(StringUtils.isEmpty(userData)) {
                returnMessage.put("success", false);
                return returnMessage.toString();
            }
            returnMessage.put("success", true);
            return returnMessage.toString();
        }
        String password = jsonObject.getString("password");
        Object objData = repository.find(userId);
        if(objData==null) {
            returnMessage.put("success", false);
            return returnMessage.toString();
        }
        jsonObject = new JSONObject(objData.toString());
        if(jsonObject.getString("password").equals(password)) {
            returnMessage.put("success", true);
            returnMessage.put("entity", objData);
            return returnMessage.toString();
        }
        returnMessage.put("success", false);
        return returnMessage.toString();
    }

    public String signUp(String entityData) {
        JSONObject jsonObject = new JSONObject(entityData);
        String email = jsonObject.getString("email");
        JSONObject returnMessage = new JSONObject();
        if(repository.find(email) != null) {
            returnMessage.put("success", false);
            return returnMessage.toString();
        }
        repository.add(email, entityData);
        returnMessage.put("success", true);
        return returnMessage.toString();
    }

    public String addReview(String review) {
        JSONObject jsonObject = new JSONObject(review);
        String query =
                "{\n" +
                        "  \"date\" : \""+jsonObject.getString("date")+"\",\n" +
                        "  \"comments\" : \""+jsonObject.getString("comments")+"\",\n" +
                        "  \"listing_id\" : "+jsonObject.getInt("listing_id")+",\n" +
                        "  \"@timestamp\" : \""+jsonObject.getString("@timestamp")+"\",\n" +
                        "  \"reviewer_name\" : \""+jsonObject.getString("reviewer_name")+"\",\n" +
                        "  \"reviewer_id\" : "+jsonObject.getString("reviewer_id")+",\n" +
                        "  \"id\" : "+jsonObject.getString("id")+"\n" +
                        "}";
        System.out.println(query);
        String url = "http://localhost:9200/airbnb_reviews/_doc/";
        JSONObject responseObject = getResponseEntity(query, url);
        System.out.println(responseObject.toString());
        String result = responseObject.getString("result");

        JSONObject returnMessage = new JSONObject();
        if(result.equalsIgnoreCase("created") || result.equalsIgnoreCase("updated")) {
            returnMessage.put("success", true);
        } else {
            returnMessage.put("success", false);
        }
        return returnMessage.toString();
    }

    public String getPropertiesByHostId(String hostId) {
        String query =
                "{\n" +
                        "  \"query\": {\n" +
                        "    \"bool\": {\n" +
                        "      \"should\": [\n" +
                        "        {\n" +
                        "          \"match\": {\n" +
                        "            \"host_id\": \""+hostId+"\"\n" +
                        "          }\n" +
                        "        }\n" +
                        "      ]\n" +
                        "    }\n" +
                        "  }\n" +
                        "}";

        return callApi(query, "airbnb").toString();
    }

    public String getBookingsByUserId(String userId) {
        String query=
                        "{\n" +
                        "  \"query\": {\n" +
                        "    \"bool\": {\n" +
                        "      \"should\": [\n" +
                        "        {\n" +
                        "          \"match\": {\n" +
                        "            \"user_id\": \""+userId+"\"\n" +
                        "          }\n" +
                        "        }\n" +
                        "      ]\n" +
                        "    }\n" +
                        "  }\n" +
                        "}";
        JSONObject jsonObject = new JSONObject();
        JSONArray userBookings = new JSONArray(callApi(query, "airbnb_bookings"));
        jsonObject.put("user_bookings", userBookings);
        List<String> ids = new ArrayList<>();
        for(int i = 0 ; i < userBookings.length() ; i++)
            ids.add(userBookings.getJSONObject(i).getJSONObject("_source").getString("property_id"));
        jsonObject.put("property_details", new JSONArray(getDataFromElastic(ids)));
        return jsonObject.toString();
    }

    public String getBookingByUserIdAndPropertyId(String userId, String id) {
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"should\": [\n" +
                "        {\"match\": {\n" +
                "          \"user_id\": \""+userId+"\"\n" +
                "        }},\n" +
                "        {\"match\": {\n" +
                "          \"property_id\": \""+id+"\"\n" +
                "        }}\n" +
                "      ]\n" +
                "    }\n" +
                "  }\n" +
                "}";

        return callApi(query, "airbnb_bookings").getJSONObject(0).toString();
    }

    public String getUser(String userId) {
        JSONObject jsonObject = new JSONObject((String)repository.find(userId));
        System.out.println(jsonObject);
        JSONObject ans = new JSONObject();
        ans.put("data", jsonObject.getString("name"));
        return ans.toString();
    }

    private JSONArray callApi(String query, String endpoint) {
        String url = "http://localhost:9200/"+endpoint+"/_search";
        JSONObject responseObject = getResponseEntity(query, url);
        return responseObject.getJSONObject("hits").getJSONArray("hits");
    }

    private JSONObject getResponseEntity(String query, String url) {
        RestTemplate template = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type","application/json");

        HttpEntity<String> resultEntity = new HttpEntity<>(query, headers);
        ResponseEntity<String> responseEntity = template.exchange(url, HttpMethod.POST, resultEntity, String.class);
        return new JSONObject(responseEntity.getBody());
    }

    public String addBooking(String data) {
        RestTemplate template = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type","application/json");

        HttpEntity<String> resultEntity = new HttpEntity<>(data, headers);
        ResponseEntity<String> responseEntity = template.exchange("http://localhost:9200/airbnb_bookings/_doc", HttpMethod.POST, resultEntity, String.class);
        JSONObject jsonObject = new JSONObject(responseEntity.getBody());
        JSONObject ans = new JSONObject();
        if(jsonObject.getString("result").equalsIgnoreCase("created") ||
                jsonObject.getString("result").equalsIgnoreCase("updated")) {
            ans.put("created", true);
        } else {
            ans.put("created", false);
        }
        return ans.toString();
    }
}

