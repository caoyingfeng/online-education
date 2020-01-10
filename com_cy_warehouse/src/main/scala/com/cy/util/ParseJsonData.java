package com.cy.util;

import com.alibaba.fastjson.JSONObject;

/**
 * @author cy
 * @create 2020-01-07 16:25
 */
public class ParseJsonData {

    public static JSONObject getJsonData(String data){
        try {
            return JSONObject.parseObject(data);  //ctrl alt t
        } catch (Exception e) {
            return null;
        }
    }
}
