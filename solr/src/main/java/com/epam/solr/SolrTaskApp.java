package com.epam.solr;

import com.epam.kerberos.HadoopKerberosUtil;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.params.AuthPolicy;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.Krb5HttpClientConfigurer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import java.io.IOException;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;

public class SolrTaskApp {
  public static void main( String[] args ) throws Exception {
    // Configuration configuration = createConfig();
    try {
      System.setProperty( "java.security.auth.login.config", "infra_solr_jaas.conf" );
      //System.setProperty( "javax.net.ssl.trustStore", "ssl_truststore" );
      //System.setProperty( "java.security.auth.login.config", "mapr.login.conf" );

      //LoginContext loginContext = HadoopKerberosUtil.doLogin( "devuser@PENTAHOQA.COM", "password" );

      /*System.setProperty( "java.security.auth.login.config", "login.conf" );
      System.setProperty( "java.security.krb5.conf", "krb5.conf" );
      System.setProperty( "javax.security.auth.useSubjectCredsOnly", "false" );*/

      //HttpClientBuilder.create().build();
      HttpClientUtil.setConfigurer( new Krb5HttpClientConfigurer() );
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set( "username", "mapr" );
      params.set( "password", "password" );
      //params.set( "qt", "/admin/info/system" );
      HttpClient testClient = HttpClientUtil.createClient( params );

      CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
      UsernamePasswordCredentials credentials = new UsernamePasswordCredentials( "devuser",
        "password" );
      credentialsProvider.setCredentials( AuthScope.ANY, credentials );
      /*credentialsProvider.setCredentials( new AuthScope( "svqxbdcn6hdp26secn3.pentahoqa.com", 8886, "PENTAHOQA.COM", AuthPolicy.SPNEGO ),
        new Credentials() {
          @Override
          public Principal getUserPrincipal() {
            return () -> "devuser";
          }

          @Override
          public String getPassword() {
            return "password";
          }
        } );*/

      HttpClientContext context = HttpClientContext.create();
      context.setCredentialsProvider( credentialsProvider );
      HttpUriRequest request = new HttpGet( "http://svqxbdcn6hdp26secn3.pentahoqa.com:8886/solr/admin/info/system" );
      //HttpUriRequest request = new HttpGet( "https://svqxbdcn6mapr52secn1.pentahoqa.com:8443/rest/dashboard/info" );

      /*HttpClientBuilder builder = HttpClientBuilder.create();

      SSLContextBuilder sslcb = new SSLContextBuilder();
      sslcb.loadTrustMaterial( KeyStore.getInstance( KeyStore.getDefaultType() ), new TrustSelfSignedStrategy() );
      builder.setSslcontext( sslcb.build() );


      builder.setDefaultCredentialsProvider( credentialsProvider );
      CloseableHttpClient client = builder.build();*/

      /*DefaultHttpClient httpclient = new DefaultHttpClient();
      testClient.getAuthSchemes().register( AuthPolicy.SPNEGO, new SPNegoSchemeFactory( true ) );
      testClient.getCredentialsProvider()
        .setCredentials( new AuthScope( null, -1, AuthScope.ANY_REALM, AuthPolicy.SPNEGO ), new Credentials() {
          @Override public Principal getUserPrincipal() {
            return null;
          }

          @Override public String getPassword() {
            return null;
          }
        } );*/

      HttpResponse tresponse = testClient.execute( request, context );
      String responseString = new BasicResponseHandler().handleResponse( tresponse );
      System.out.println( responseString );
     // SolrServer solr = new CommonsHttpSolrServer( "http://svqxbdcn6hdp26secn3.pentahoqa.com:8886/solr" );

      /*QueryResponse response = Subject.doAs( loginContext.getSubject(), new PrivilegedExceptionAction<QueryResponse>() {
        @Override
        public QueryResponse run() throws Exception {
          ModifiableSolrParams params = new ModifiableSolrParams();
          params.set( "qt", "/admin/info/system" );
          return solr.query( params );
        }
      } );*/

      // http://localhost:8983/solr/spellCheckCompRH?q=epod&spellcheck=on&spellcheck.build=true
      /*ModifiableSolrParams paramss = new ModifiableSolrParams();
      paramss.set( "qt", "/admin/info/system" );

      QueryResponse responses = solr.query( paramss );
      System.out.println( "response = " + responses );*/
    } catch ( /*LoginException | */IOException e ) {
      e.printStackTrace();
    }


  }
}
