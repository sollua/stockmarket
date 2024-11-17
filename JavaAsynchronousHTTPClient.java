package StockMarket;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import org.json.JSONObject;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
/**
 * @author Crunchify.com
 * Overview and Simple Java Asynchronous HttpClient Client Tutorial
 */
public class JavaAsynchronousHTTPClient {
    private static final HttpClient JsonHttpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    // HttpClient: An HttpClient can be used to send requests and retrieve their responses. An HttpClient is created through a builder.
    // Duration: A time-based amount of time, such as '5 seconds'.
    public static void main(String[] args) {
        // Async HTTPClient Example
        JsonAsyncHTTPClient();
    }
    private static void JsonAsyncHTTPClient() {
        HttpRequest JsonRequest = HttpRequest.newBuilder()
                .GET()
                .uri(URI.create("https://yahoo-finance127.p.rapidapi.com/key-statistics/yinn"))
            	.setHeader("x-rapidapi-key", "0257c6a1dbmshca0aee5069c2a78p1fff76jsnd7942c0c2a20")
            	.setHeader("x-rapidapi-host", "yahoo-finance127.p.rapidapi.com")
                .build();
        CompletableFuture<HttpResponse<String>> JsonAsyncResponse = null;
        // sendAsync(): Sends the given request asynchronously using this client with the given response body handler.
        //Equivalent to: sendAsync(request, responseBodyHandler, null).
        JsonAsyncResponse = JsonHttpClient.sendAsync(JsonRequest, HttpResponse.BodyHandlers.ofString());
        String JsonAsyncResultBody = null;
        int JsonAsyncResultStatusCode = 0;
        try {
            JsonAsyncResultBody = JsonAsyncResponse.thenApply(HttpResponse::body).get(5, TimeUnit.SECONDS);
            
            // OR:
            
            // join(): Returns the result value when complete, or throws an (unchecked) exception if completed exceptionally. 
            // To better conform with the use of common functional forms, 
            // if a computation involved in the completion of this CompletableFuture threw an exception, 
            // this method throws an (unchecked) CompletionException with the underlying exception as its cause.
            
            HttpResponse<String> response = JsonAsyncResponse.join();
            JsonAsyncResultStatusCode = JsonAsyncResponse.thenApply(HttpResponse::statusCode).get(5, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
        }
        JsonPrint("=============== AsyncHTTPClient Body:===============  \n" + JsonAsyncResultBody);
        JsonPrint("\n=============== AsyncHTTPClient Status Code:===============  \n" + JsonAsyncResultStatusCode);
        
//        BufferedReader br = new BufferedReader(new InputStreamReader(seatURL.openStream(),Charset.forName("UTF-8")));
//        String readAPIResponse = " ";
//        StringBuilder jsonString = new StringBuilder();
//
//			while((readAPIResponse = br.readLine()) != null){
//    		jsonString.append(readAPIResponse);
//			}
        String jsonObjString = JSONObject.quote(JsonAsyncResultBody.toString());
        System.out.println(jsonObjString);
        JSONObject jsonObj = new JSONObject(JsonAsyncResultBody.toString());
        System.out.println(jsonObj);
        
        Date dd= new Date(1718305823);
        System.out.println(dd);
        
        Timestamp ts=new Timestamp(1718212181);
        System.out.println(ts);
        
        LocalDateTime dateTime = LocalDateTime.ofEpochSecond(1718305823, 0, ZoneOffset.UTC);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("EEEE,MM,d,yyyy h:mm,a", Locale.ENGLISH);
        String formattedDate = dateTime.format(formatter);
        System.out.println(formattedDate); // Tuesday,November 1,2011 12:00,AM
       
       
        
    }
    private static void JsonPrint(Object data) {
        System.out.println(data);
    }
}



