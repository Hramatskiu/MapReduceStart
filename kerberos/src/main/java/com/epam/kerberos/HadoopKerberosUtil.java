package com.epam.kerberos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.IOException;

public class HadoopKerberosUtil {
  public static LoginContext doLogin( String principal, String password ) throws LoginException, IOException {
    Configuration configuration = new Configuration();
    configuration.set( "hadoop.security.authentication",
      UserGroupInformation.AuthenticationMethod.KERBEROS.toString().toLowerCase() );
    UserGroupInformation.setConfiguration( configuration );
    LoginContext loginContext = KerberosUtil.getLoginContextFromUsernamePassword( principal, password );

    loginContext.login();
    UserGroupInformation.loginUserFromSubject( loginContext.getSubject() );

    return loginContext;
  }
}
