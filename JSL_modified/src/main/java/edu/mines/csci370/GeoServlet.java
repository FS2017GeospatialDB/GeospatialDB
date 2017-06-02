package edu.mines.csci370;

import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.server.TServlet;

import edu.mines.csci370.api.GeolocationService;

public class GeoServlet extends TServlet {

   public GeoServlet()  {
      super(new GeolocationService.Processor(
            new GeoHandler()),
            new TJSONProtocol.Factory());
  }
}
