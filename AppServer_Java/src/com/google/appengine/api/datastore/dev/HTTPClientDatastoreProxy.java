package com.google.appengine.api.datastore.dev;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;

import com.google.appengine.api.users.User;
import com.google.appengine.api.users.UserService;
import com.google.appengine.api.users.UserServiceFactory;
import com.google.appengine.repackaged.com.google.io.protocol.ProtocolMessage;
import com.google.appengine.repackaged.org.apache.commons.httpclient.HttpStatus;
import com.google.apphosting.utils.remoteapi.RemoteApiPb.Request;
import com.google.apphosting.utils.remoteapi.RemoteApiPb.Response;
import com.google.apphosting.api.ApiProxy;
import com.google.apphosting.datastore.DatastoreV3Pb;

public class HTTPClientDatastoreProxy
{
    private static final Logger logger                              = Logger.getLogger(HTTPClientDatastoreProxy.class.getName());

    private DefaultHttpClient   client                              = null;
    private String              url                                 = null;
    private final int           MAX_TOTAL_CONNECTIONS               = 200;
    private final int           MAX_CONNECTIONS_PER_ROUTE           = 20;
    private final int           MAX_CONNECTIONS_PER_ROUTE_LOCALHOST = 80;
    private final int           INPUT_STREAM_SIZE                   = 10240;
    private final String        APPDATA_HEADER                      = "AppData";
    private final String        SERVICE_NAME                        = "datastore_v3";
    private final String        PROTOCOL_BUFFER_HEADER              = "ProtocolBufferType";
    private final String        PROTOCOL_BUFFER_VALUE               = "Request";

    public HTTPClientDatastoreProxy( String host, int port, boolean isSSL )
    {
        SchemeRegistry schemeRegistry = new SchemeRegistry();
        schemeRegistry.register(new Scheme("http", PlainSocketFactory.getSocketFactory(), port));
        ThreadSafeClientConnManager cm = new ThreadSafeClientConnManager(schemeRegistry);
        cm.setMaxTotal(MAX_TOTAL_CONNECTIONS);
        cm.setDefaultMaxPerRoute(MAX_CONNECTIONS_PER_ROUTE);
        url = "http://" + host + ":" + port + "/";
        HttpHost localhost = new HttpHost(url);
        cm.setMaxForRoute(new HttpRoute(localhost), MAX_CONNECTIONS_PER_ROUTE_LOCALHOST);
        client = new DefaultHttpClient(cm);
    }

    public void doPost( String appId, String method, ProtocolMessage<?> request, ProtocolMessage<?> response )
    {
        HttpPost post = new HttpPost(url);
        post.addHeader(PROTOCOL_BUFFER_HEADER, PROTOCOL_BUFFER_VALUE);
        String tag = appId;
        ApiProxy.Environment environment = ApiProxy.getCurrentEnvironment();
        User user = null;
        if (environment != null)
        {
            user = getUser();
        }
        if (user != null)
        {
            tag += ":" + user.getEmail();
            tag += ":" + user.getNickname();
            tag += ":" + user.getAuthDomain();
        }
        post.addHeader(APPDATA_HEADER, tag);

        Request remoteRequest = new Request();
        remoteRequest.setMethod(method);
        remoteRequest.setServiceName(SERVICE_NAME);
        remoteRequest.setRequestAsBytes(request.toByteArray());

        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        try
        {
            bao.write(remoteRequest.toByteArray());
            ByteArrayEntity entity = new ByteArrayEntity(bao.toByteArray());
            post.setEntity(entity);
            bao.close();
        }
        catch (IOException e1)
        {
            e1.printStackTrace();
        }

        Response remoteResponse = new Response();
        try
        {
            byte[] bytes = client.execute(post, new ByteArrayResponseHandler());
            remoteResponse.parseFrom(bytes);
        }
        catch (ClientProtocolException e)
        {
            e.printStackTrace();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        if (!remoteResponse.hasResponse()) {
            throw new ApiProxy.ApplicationException(DatastoreV3Pb.Error.ErrorCode.INTERNAL_ERROR.getValue(),
                    "The datastore did not return a response");
        }
        if (remoteResponse.hasApplicationError())
        {
            throw new ApiProxy.ApplicationException(remoteResponse.getApplicationError().getCode(),
                    remoteResponse.getApplicationError().getDetail());
        }
        if (remoteResponse.hasException())
        {
            logger.log(Level.WARNING, "Exception in " + method + " method! " + remoteResponse.getException());
        }
        if (remoteResponse.hasJavaException())
        {
            logger.log(Level.WARNING, "Java exception in " + method + " method! " + remoteResponse.getJavaException());
        }
        response.parseFrom(remoteResponse.getResponseAsBytes());
    }

    private User getUser()
    {
        UserService userService = UserServiceFactory.getUserService();
        return userService.getCurrentUser();
    }

    private class ByteArrayResponseHandler implements ResponseHandler<byte[]>
    {

        public byte[] handleResponse( HttpResponse response ) throws ClientProtocolException, IOException
        {
            int responseCode = response.getStatusLine().getStatusCode();
            if (responseCode != HttpStatus.SC_OK) {
                throw new ApiProxy.ApplicationException(DatastoreV3Pb.Error.ErrorCode.INTERNAL_ERROR.getValue(),
                        "The datastore returned an invalid response code: " + responseCode);
            }

            HttpEntity entity = response.getEntity();
            if (entity != null)
            {
                InputStream inputStream = entity.getContent();
                try
                {
                    return inputStreamToArray(inputStream);
                }
                finally
                {
                    entity.getContent().close();
                }
            }
            return new byte[] {};
        }
    }

    private byte[] inputStreamToArray( InputStream in )
    {
        int len;
        int size = INPUT_STREAM_SIZE;
        byte[] buf = null;
        try
        {
            if (in instanceof ByteArrayInputStream)
            {
                size = in.available();
                buf = new byte[size];
                len = in.read(buf, 0, size);
            }
            else
            {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                buf = new byte[size];
                while ((len = in.read(buf, 0, size)) != -1)
                {
                    bos.write(buf, 0, len);
                }
                buf = bos.toByteArray();

            }
            in.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return buf;
    }

    // used for setting this as a mock object for unit tests
    public void setClient( DefaultHttpClient client )
    {
        this.client = client;
    }
}
