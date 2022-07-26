package com.elastic.elastictest.controller;

import com.elastic.elastictest.dto.ErrorsDTO;
import com.elastic.elastictest.service.ElasticService;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import javax.websocket.server.PathParam;
import java.lang.reflect.Array;
import java.util.List;

@RestController
@RequestMapping("/elastic")
public class ElasticSearchController {

    @Autowired
    private ElasticService service;

//    @GetMapping("/search")
//    public String getDataFromElastic(@RequestParam("field") String field, @RequestParam("data") String data) {
//        return service.getDataFromElastic(field, data);
//    }

    @GetMapping("/search")
    public String getDataFromElastic(@RequestParam("field") String field, @RequestParam("data") String data) {
        if(field==null || data == null || StringUtils.isEmpty(field) || StringUtils.isEmpty(data))
            return null;
        return service.getDataFromElastic(field, data);
    }

    @GetMapping("/getProperty")
    public String getProperty(@RequestParam("id") String id) {
        if(id==null || StringUtils.isEmpty(id))
            return null;
        return service.getDataFromElastic(id);
    }

    @GetMapping("/getProperties")
    public String getProperty(@RequestParam("ids") List<String> ids) {
        if(ids==null || ids.size()==0)
            return null;
        return service.getDataFromElastic(ids);
    }

    @GetMapping("/reviews")
    public String getReviews(@RequestParam("id") String id) {
        if(id==null || StringUtils.isEmpty(id))
            return null;
        return service.getReviews(id);
    }

    @PostMapping("/signin")
    public String signIn(@RequestBody String user) {

        if(user==null || StringUtils.isEmpty(user))
            return null;
        return service.signIn(user);
    }

    @PostMapping("/signup")
    public String signUp(@RequestBody String userData) {
        System.out.println(userData);
//        RestTemplate template = new RestTemplate();
//        HttpHeaders headers = new HttpHeaders();
//        headers.add("Content-Type","application/json");
//        headers.add("Cookie", "PHPSESSID=u0mli6od6i6pkdl5vfitgfj19c");
//
//        HttpEntity<String> resultEntity = new HttpEntity<>(userData, headers);
//        ResponseEntity<String> responseEntity = template.exchange("http://oms-dhvp.fermion.in/v1/order/journey-status", HttpMethod.POST, resultEntity, String.class);

//        JSONObject jsonObject = new JSONObject(responseEntity.getBody());
        JSONObject jsonObject = new JSONObject(userData);
//        Object ans = new Object();

        if(!jsonObject.has("result")  || jsonObject.get("result") == null) {
            System.out.println("True");
        }
        System.out.println(jsonObject.has("result"));
        System.out.println(jsonObject.isNull("result"));
        System.out.println(jsonObject.get("result")=="null");
//        if(jsonObject.has("errors")) {
//            if(jsonObject.get("errors") instanceof Integer) {
//                Integer errorNum = (Integer)jsonObject.get("errors");
//                System.out.println(errorNum);
//            } else {
//                JSONObject json = jsonObject.getJSONObject("errors");
//                if(json != null) {
//                    ans = (Object) json;
//                } else {
//                    JSONArray jsonArray = jsonObject.getJSONArray("errors");
//                    ans = jsonArray.toList();
//                }
////                ErrorsDTO orderStatusErrors = new ErrorsDTO();
////                orderStatusErrors.setDescription(jsonObject.getJSONObject("errors").getString("description"));
////                orderStatusErrors.setMessage(jsonObject.getJSONObject("errors").getString("message"));
////                System.out.println(orderStatusErrors∂);
//            }
//        }
//        System.out.println(ans );

//        if(userData==null || StringUtils.isEmpty(userData))
//            return null;
//        return service.signUp(userData);
        return "";
    }

    @PostMapping("/addReview")
    public String addReview(@RequestBody String review) {
        if(review==null || StringUtils.isEmpty(review))
            return null;
        return service.addReview(review);
    }

    @GetMapping("/getProperties/{hostId}")
    public String getPropertiesByHostId(@PathVariable String hostId) {
        if(hostId==null || StringUtils.isEmpty(hostId))
            return null;
        return service.getPropertiesByHostId(hostId);
    }

    @GetMapping("/getBookings/{userId}")
    public String getBookingsByUserId(@PathVariable String userId) {
        if(userId==null || StringUtils.isEmpty(userId))
            return null;
        return service.getBookingsByUserId(userId);
    }

    @GetMapping("/getBooking/{userId},{id}")
    public String getBookingByUserIdAndPropertyId(@PathVariable String userId, @PathVariable String id) {
        if(userId==null || StringUtils.isEmpty(userId) || id==null || StringUtils.isEmpty(id))
            return null;
        return service.getBookingByUserIdAndPropertyId(userId, id);
    }

    @GetMapping("/getUser/{userId}")
    public String getUser(@PathVariable String userId) {
        if(userId==null || StringUtils.isEmpty(userId))
            return null;
        return service.getUser(userId);
    }

    @PostMapping("/addBooking")
    public String addBooking(@RequestBody String data) {
        JSONObject jsonObject = new JSONObject(data);
        return service.addBooking(jsonObject.toString());
    }
}
