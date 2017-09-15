package com.epam.zookeeper;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class ZookeeperTaskApp {
  public static void main( String[] args ) {
    Socket socket;
    String command = "dump";
    String host = "svqxbdcn6mapr52secn1.pentahoqa.com";
    Integer port = 5181;
    List<String> lines = new ArrayList<>(  );
    try {
      socket = new Socket( InetAddress.getByName(host), port);
    } catch (final UnknownHostException e) {
      throw new IllegalStateException("could not connect to host: " + host + " and port: " + port, e);
    } catch (final IOException e) {
      throw new IllegalStateException("could not connect to host: " + host + " and port: " + port, e);
    }

    try {
      IOUtils.write(command + "\n", socket.getOutputStream());
    } catch (final IOException e) {
      throw new IllegalStateException("could not execute command to host: " + host + " and port: " + port + ", command: " + command, e);
    }

    try {
      lines = IOUtils.readLines(socket.getInputStream());
    } catch (final IOException e) {
      throw new IllegalStateException("could not connect to host: " + host + " and port: " + port, e);
    } finally {
      try {
        socket.close();
      } catch (final IOException e) {
        throw new IllegalStateException("Error disconnecting from host", e);
      }
    }

    lines.forEach( System.out::println );
  }
}
