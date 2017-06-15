package edu.mines.csci370;

import edu.mines.csci370.api.GeolocationService;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.server.TServlet;

public class GeoServlet extends TServlet {

   public GeoServlet()  {
      super(new GeolocationService.Processor(
            new GeoHandler()),
            new TJSONProtocol.Factory());
  }
}
