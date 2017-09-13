package com.epam.oozie;

import java.io.IOException;
import java.util.Properties;

public class OozieConfigLoader {
  public void loadConfigFromXml( Properties configs, String pathToConfigFile ) throws IOException {
    Class current = getClass();
    configs.loadFromXML( getClass().getResourceAsStream( pathToConfigFile ) );
  }
}
