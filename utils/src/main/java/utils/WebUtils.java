package utils;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.http.NameValuePair;

import java.util.List;


public class WebUtils {
    public static HttpClient client = new HttpClient();
    /*
     * Does the actual call of the Web Service, using a HttpClient which executes the GetMethod.
     */
    public static String request(String url) {
        Client client1 = Client.create();
        System.out.println(url);
        
        WebResource wbres = client1.resource(url);
        ClientResponse cr = wbres.accept("application/json").get(ClientResponse.class);
        int status = cr.getStatus();
        String response = cr.getEntity(String.class);
        
        if(status == 200)
            return response;
        
        return "";
    }

    public static String post(String url, List<NameValuePair> urlParameters)  {
        try{

            PostMethod method = new PostMethod(url);
            // add header
            for(NameValuePair name_val:urlParameters){
                method.addParameter(name_val.getName(), name_val.getValue());
            }

            //Set the results type, which will be JSON.
            method.addRequestHeader(new Header("Accept", "application/json"));
            method.addRequestHeader(new Header("content-type", "application/x-www-form-urlencoded"));

            int response = client.executeMethod(method);
            if (response != HttpStatus.SC_OK) {
                System.out.println("Method failed: " + method.getStatusText());
            }
            // Read the response body.
            byte[] responseBody = method.getResponseBody();

            // Deal with the response.
            // Use caution: ensure correct character encoding and is not binary data
            String response_str = new String(responseBody);
            System.out.println(response_str);

            return response_str;
        }
        catch(Exception e){
            System.out.println(e.getMessage());
        }
        return "";
    }
}
