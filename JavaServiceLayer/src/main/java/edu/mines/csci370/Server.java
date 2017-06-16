package edu.mines.csci370;

import edu.mines.csci370.api.GeolocationService;
import org.apache.http.*;
import org.apache.http.entity.EntityTemplate;
import org.apache.http.impl.DefaultHttpResponseFactory;
import org.apache.http.impl.DefaultHttpServerConnection;
import org.apache.http.impl.NoConnectionReuseStrategy;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.*;
import org.apache.http.util.EntityUtils;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TMemoryBuffer;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * We uses a Javascript client as our front-end project. As thrift JS library only supports web-socket and xml http request.
 * we cannot use native socket to communicate with the front-end application. As a workaround, we built a small, but fully
 * functional http server to respond the request from the front-end application.
 */

public class Server {

    /**
     * Start the http server and listen to port 8000
     */
    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("No config file applied, using the default values...");
            System.err.println("The sample config file is located under the root dir of JSL");
            Database.initialize(null);
        } else
            Database.initialize(args[0]);

        try {
            Thread t = new RequestListenerThread(8000);
            t.setDaemon(false);
            t.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * The main thread which listen to the http request. When a request is received, a new thread is spawned to handle
     * the request.
     */
    static class RequestListenerThread extends Thread {

        private final ServerSocket httpServerSocket;
        private final HttpParams params;
        private final HttpService httpService;

        public RequestListenerThread(int port) throws IOException {
            this.httpServerSocket = new ServerSocket(port);
            this.params = new BasicHttpParams();
            this.params.setIntParameter(CoreConnectionPNames.SOCKET_BUFFER_SIZE, 8 * 1024).setParameter(CoreProtocolPNames.ORIGIN_SERVER, "HttpComponents/1.1")
                    .setBooleanParameter(CoreConnectionPNames.STALE_CONNECTION_CHECK, false).setBooleanParameter(CoreConnectionPNames.TCP_NODELAY, true);

            // Set up the HTTP protocol processor
            HttpProcessor httpproc = new BasicHttpProcessor();

            // Set up request handlers
            HttpRequestHandlerRegistry reqistry = new HttpRequestHandlerRegistry();
            reqistry.register("*", new ThriftRequestHandler());

            // Set up the HTTP service
            this.httpService = new HttpService(httpproc, new NoConnectionReuseStrategy(), new DefaultHttpResponseFactory());
            this.httpService.setParams(this.params);
            this.httpService.setHandlerResolver(reqistry);
        }

        public void run() {
            System.out.println("HTTP listening on port " + httpServerSocket.getLocalPort());

            while (!Thread.interrupted()) {
                try {
                    // Set up HTTP connection
                    Socket socket = this.httpServerSocket.accept();
                    DefaultHttpServerConnection conn = new DefaultHttpServerConnection();
                    conn.bind(socket, this.params);

                    // Start worker thread
                    Thread t = new Thread(() -> {
                        try {
                            HttpContext context = new BasicHttpContext(null);
                            while (!Thread.interrupted() && conn.isOpen())
                                httpService.handleRequest(conn, context);
                        } catch (IOException | HttpException e) {
                            System.err.println(e.getMessage());
                        } finally {
                            try {
                                conn.shutdown();
                            } catch (IOException ignore) {
                            }
                        }
                    });
                    t.setDaemon(true);
                    t.start();

                } catch (InterruptedIOException ex) {
                    break;
                } catch (IOException e) {
                    System.out.println("I/O error initialising connection thread: " + e.getMessage());
                    break;
                }
            }
        }
    }

    /**
     * Thrift protocol handler.
     */
    static class ThriftRequestHandler implements HttpRequestHandler {
        public void handle(final HttpRequest request, final HttpResponse response, final HttpContext context) throws HttpException, IOException {

            // Check the HTTP Method
            String method = request.getRequestLine().getMethod().toUpperCase();
            if (!method.equals("GET") && !method.equals("POST") && !method.equals("OPTIONS")) {
                throw new MethodNotSupportedException(method + " method not supported");
            }

            // Handle "service" Requests
            String target = request.getRequestLine().getUri();
            if (target.startsWith("/service") && method.equals("POST")) {
                System.out.println((new java.util.Date()) + ": Incoming request => " + target);
                HttpEntity entity = ((HttpEntityEnclosingRequest) request).getEntity();
                byte[] entityContent = EntityUtils.toByteArray(entity);

                final String output = this.thriftRequest(entityContent);
                EntityTemplate body = new EntityTemplate(outstream -> {
                    OutputStreamWriter writer = new OutputStreamWriter(outstream, "UTF-8");
                    writer.write(output);
                    writer.flush();
                });

                body.setContentType("text/html; charset=UTF-8");
                response.setEntity(body);
                response.addHeader("Access-Control-Allow-Origin", "*");
            }

            // Handle CORS OPTIONS Requests
            else if (method.equals("OPTIONS")) {
                response.addHeader("Access-Control-Allow-Headers", "Origin,X-Requested-With,Content-Type,Accept");
                response.addHeader("Access-Control-Allow-Origin", "*");
                response.addHeader("Allow", "GET,POST,OPTIONS");
            }
        }

        private String thriftRequest(byte[] input) {
            try {
                //Input
                TMemoryBuffer inbuffer = new TMemoryBuffer(input.length);
                inbuffer.write(input);
                TProtocol inprotocol = new TJSONProtocol(inbuffer);

                //Output
                TMemoryBuffer outbuffer = new TMemoryBuffer(0);
                TProtocol outprotocol = new TJSONProtocol(outbuffer);

                GeoHandler handler = new GeoHandler();
                TProcessor processor = new GeolocationService.Processor<>(handler);
                processor.process(inprotocol, outprotocol);

                byte[] output = new byte[outbuffer.length()];
                outbuffer.readAll(output, 0, output.length);

                return new String(output, "UTF-8");
            } catch (Throwable t) {
                t.printStackTrace();
                return "Error:" + t.getMessage();
            }
        }
    }
}
