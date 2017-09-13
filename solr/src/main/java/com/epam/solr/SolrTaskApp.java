package com.epam.solr;

import com.epam.kerberos.HadoopKerberosUtil;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.Krb5HttpClientConfigurer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public class SolrTaskApp {
  public static void main( String[] args ) throws Exception {
    // Configuration configuration = createConfig();
    try {
      System.setProperty( "java.security.auth.login.config", "infra_solr_jaas.conf" );
      LoginContext loginContext = HadoopKerberosUtil.doLogin( "devuser@PENTAHOQA.COM", "password" );

      /*System.setProperty( "java.security.auth.login.config", "login.conf" );
      System.setProperty( "java.security.krb5.conf", "krb5.conf" );
      System.setProperty( "javax.security.auth.useSubjectCredsOnly", "false" );*/

      HttpClientUtil.setConfigurer( new Krb5HttpClientConfigurer() );
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set( "username", "devuser" );
      params.set( "password", "password" );
      //params.set( "qt", "/admin/info/system" );
      HttpClient testClient = HttpClientUtil.createClient( params );

      HttpUriRequest request = new HttpGet( "http://svqxbdcn6hdp26secn3.pentahoqa.com:8886/solr/admin/info/system" );

      /*HttpClientBuilder builder = HttpClientBuilder.create();

      SSLContextBuilder sslcb = new SSLContextBuilder();
      sslcb.loadTrustMaterial( KeyStore.getInstance( KeyStore.getDefaultType() ), new TrustSelfSignedStrategy() );
      builder.setSslcontext( sslcb.build() );

      CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
      UsernamePasswordCredentials credentials = new UsernamePasswordCredentials( "infra-solr@PENTAHOQA.COM",
      "password" );
      credentialsProvider.setCredentials( AuthScope.ANY, credentials );
      credentialsProvider.setCredentials( new AuthScope( null, -1, AuthScope.ANY_REALM, AuthPolicy.SPNEGO ),
        new Credentials() {
          @Override
          public Principal getUserPrincipal() {
            return null;
          }

          @Override
          public String getPassword() {
            return null;
          }
        } );
      builder.setDefaultCredentialsProvider( credentialsProvider );
      CloseableHttpClient client = builder.build();*/

      /*DefaultHttpClient httpclient = new DefaultHttpClient();
      httpclient.getAuthSchemes().register( AuthPolicy.SPNEGO, new SPNegoSchemeFactory( true ) );
      httpclient.getCredentialsProvider()
        .setCredentials( new AuthScope( null, -1, AuthScope.ANY_REALM, AuthPolicy.SPNEGO ), new Credentials() {
          @Override public Principal getUserPrincipal() {
            return null;
          }

          @Override public String getPassword() {
            return null;
          }
        } );*/

      //HttpResponse tresponse = testClient.execute( request );
      //String responseString = new BasicResponseHandler().handleResponse( tresponse );
      SolrServer solr = new CommonsHttpSolrServer( "http://svqxbdcn6hdp26secn3.pentahoqa.com:8886/solr" );

      QueryResponse response = Subject.doAs( loginContext.getSubject(), new PrivilegedExceptionAction<QueryResponse>() {
        @Override
        public QueryResponse run() throws Exception {
          ModifiableSolrParams params = new ModifiableSolrParams();
          params.set( "qt", "/admin/info/system" );
          return solr.query( params );
        }
      } );

      // http://localhost:8983/solr/spellCheckCompRH?q=epod&spellcheck=on&spellcheck.build=true
      ModifiableSolrParams paramss = new ModifiableSolrParams();
      paramss.set( "qt", "/admin/info/system" );

      QueryResponse responses = solr.query( paramss );
      System.out.println( "response = " + responses );
    } catch ( /*LoginException | */IOException e ) {
      e.printStackTrace();
    }


  }
}
