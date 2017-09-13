package com.epam.sqoop.util;

import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MInput;

import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.ResourceBundle;

public class SqoopUtil {
  public static void describe( List<MConfig> configs, ResourceBundle resource ) {
    configs.stream()
      .collect( HashMap<String, List<MInput<?>>>::new, ( m, c ) -> m.put( c.getLabelKey(), c.getInputs() ),
        ( m, u ) -> {
        } )
      .forEach( ( k, v ) -> {
        System.out.println( k );
        v.stream()
          .collect( HashMap<String, Object>::new, ( m, c ) -> m.put( c.getLabelKey(), c.getValue() ), ( m, u ) -> {
          } )
          .forEach( ( k1, v1 ) -> System.out.println( resource.getString( k1 ) + ":" + v1 ) );
        System.out.println();
      } );
  }

  public static void printValidationMessages( List<MConfig> configs ) {
    configs.stream().map( MConfig::getValidationMessages )
      .forEach( messages -> messages.stream().filter( Objects::nonNull )
        .forEach( System.out::println ) );
    configs.stream().map( MConfig::getInputs )
      .forEach( mInputs -> mInputs.stream().map( MInput::getValidationMessages )
        .forEach( messages -> messages.stream().filter( Objects::nonNull )
          .forEach( System.out::println ) ) );
  }

}
