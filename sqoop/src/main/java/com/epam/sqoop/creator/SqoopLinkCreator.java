package com.epam.sqoop.creator;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MLinkConfig;

import java.util.Map;

public class SqoopLinkCreator {
  public static MLink createLink( SqoopClient sqoopClient, String connectorName, String name, String user,
                                  Map<String, String> configMap ) {
    MLink link = sqoopClient.createLink( connectorName );
    link.setName( name );
    link.setCreationUser( user );
    MLinkConfig linkConfig = link.getConnectorLinkConfig();

    configMap.forEach( ( key, value ) -> linkConfig.getStringInput( key ).setValue( value ) );

    return link;
  }
}
