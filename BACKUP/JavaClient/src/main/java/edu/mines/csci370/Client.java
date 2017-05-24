package edu.mines.csci370;

import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import edu.mines.csci370.api.Feature;
import edu.mines.csci370.api.GeolocationService;

public class Client {
  public static void main(String [] args) {

   
    try {
      TTransport transport;
     
      transport = new TSocket("localhost", 8000);
      transport.open();

      TProtocol protocol = new TBinaryProtocol(transport);
      GeolocationService.Client client = new GeolocationService.Client(protocol);

      perform(client);

      transport.close();
    } catch (TException x) {
      x.printStackTrace();
    } 
  }

  private static void perform(GeolocationService.Client client) throws TException {
      List<Feature> features = client.getFeatures(-180, 180, -90, 90);
      System.out.println(features);
  }
}
