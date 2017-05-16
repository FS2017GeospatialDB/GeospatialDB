package edu.mines.csci370;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URLDecoder;
import java.util.Collection;
import java.util.Iterator;
import java.util.Locale;

import org.apache.http.ConnectionClosedException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpServerConnection;
import org.apache.http.HttpStatus;
import org.apache.http.MethodNotSupportedException;
import org.apache.http.entity.ContentProducer;
import org.apache.http.entity.EntityTemplate;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.DefaultHttpResponseFactory;
import org.apache.http.impl.DefaultHttpServerConnection;
import org.apache.http.impl.NoConnectionReuseStrategy;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.BasicHttpProcessor;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpProcessor;
import org.apache.http.protocol.HttpRequestHandler;
import org.apache.http.protocol.HttpRequestHandlerRegistry;
import org.apache.http.protocol.HttpService;
import org.apache.http.util.EntityUtils;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TServer.Args;

import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.TProcessor;

import edu.mines.csci370.api.GeolocationService;

public class Server {

	private static GeoHandler handler;
	private static GeolocationService.Processor processor;

	public static void main(String[] args) {
		Database.initialize();
		
    try {
      Thread t = new RequestListenerThread(8000);
      t.setDaemon(false);
      t.start();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  static class ThriftRequestHandler implements HttpRequestHandler { 
    public void handle(final HttpRequest request, final HttpResponse response, final HttpContext context) throws HttpException, IOException {

      // Check the HTTP Method
      String method = request.getRequestLine().getMethod().toUpperCase();
      if (!method.equals("GET") && !method.equals("HEAD") && !method.equals("POST")) {
          throw new MethodNotSupportedException(method + " method not supported");
      }

      // Handle "service" Requests
      String target = request.getRequestLine().getUri();
      if (target.indexOf("?") != -1) target = target.substring(0, target.indexOf("?"));
      if (target.equals("/service")) {
       
        HttpEntity entity = ((HttpEntityEnclosingRequest) request).getEntity();
        byte[] entityContent = EntityUtils.toByteArray(entity);
          
        final String output = this.thriftRequest(entityContent);
        System.out.println(output);
        EntityTemplate body = new EntityTemplate(new ContentProducer() {

          public void writeTo(final OutputStream outstream) throws IOException {
            OutputStreamWriter writer = new OutputStreamWriter(outstream, "UTF-8");
            writer.write(output);
            writer.flush();
          }
        });
          
        body.setContentType("text/html; charset=UTF-8");
        response.setEntity(body);
      }
    }
 
    private String thriftRequest(byte[] input) {
      try {
          //Input
          TMemoryBuffer inbuffer = new TMemoryBuffer(input.length);           
          inbuffer.write(input);              
          TProtocol  inprotocol   = new TJSONProtocol(inbuffer);                   
          
          //Output
          TMemoryBuffer outbuffer = new TMemoryBuffer(0);           
          TProtocol outprotocol   = new TJSONProtocol(outbuffer);
          
          GeoHandler handler = new GeoHandler();
          TProcessor processor = new GeolocationService.Processor<GeoHandler>(handler);      
          processor.process(inprotocol, outprotocol);
          
          byte[] output = new byte[outbuffer.length()];
          outbuffer.readAll(output, 0, output.length);
      
          return new String(output,"UTF-8");
      } catch(Throwable t){
          t.printStackTrace();
          return "Error:"+t.getMessage();
      } 
    }
  }

  static class RequestListenerThread extends Thread {

      private final ServerSocket httpServerSocket;
      private final ServerSocket rawServerSocket;
      private final HttpParams params;
      private final HttpService httpService;

      public RequestListenerThread(int port) throws IOException {
          this.httpServerSocket = new ServerSocket(port);
          this.rawServerSocket = new ServerSocket(port+1);
          this.params = new BasicHttpParams();
          this.params.setIntParameter(CoreConnectionPNames.SO_TIMEOUT, 1000).setIntParameter(CoreConnectionPNames.SOCKET_BUFFER_SIZE, 8 * 1024)
                  .setBooleanParameter(CoreConnectionPNames.STALE_CONNECTION_CHECK, false).setBooleanParameter(CoreConnectionPNames.TCP_NODELAY, true)
                  .setParameter(CoreProtocolPNames.ORIGIN_SERVER, "HttpComponents/1.1");

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
        System.out.println("Point your browser to http://localhost:8088/index.html");
        
        while (!Thread.interrupted()) {
          try {
              // Set up HTTP connection
              Socket socket = this.httpServerSocket.accept();
              DefaultHttpServerConnection conn = new DefaultHttpServerConnection();
              System.out.println("Incoming connection from " + socket.getInetAddress());
              conn.bind(socket, this.params);

              // Start worker thread
              Thread t = new Thread(new Runnable() {
                @Override public void run() {
                  try {
                    HttpContext context = new BasicHttpContext(null);
                    while (!Thread.interrupted() && conn.isOpen())
                      httpService.handleRequest(conn, context);
                  } catch (IOException | HttpException e) {
                    System.err.println(e.getMessage());
                  } finally {
                    try {conn.shutdown();}
                    catch (IOException ignore) {}
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
  }}

  /*static class HttpWorkerThread extends Thread {

      private final HttpService httpservice;
      private final HttpServerConnection conn;
         

      public HttpWorkerThread(final HttpService httpservice, final HttpServerConnection conn) {
          super();
          this.httpservice = httpservice;
          this.conn = conn;
      }

      public void run() {
          System.out.println("New HTTP connection thread");
          HttpContext context = new BasicHttpContext(null);
          try {
            while (!Thread.interrupted() && this.conn.isOpen())
              this.httpservice.handleRequest(this.conn, context);
          } catch (ConnectionClosedException ex) {
            System.out.println("Client closed connection");
          } catch (IOException ex) {
            System.out.println("I/O error: " + ex.getMessage());
          } catch (HttpException ex) {
            System.out.println("Unrecoverable HTTP protocol violation: " + ex.getMessage());
          } finally {
              try {
                  this.conn.shutdown();
              } catch (IOException ignore) {
              }
          }
      }
  }*/
}