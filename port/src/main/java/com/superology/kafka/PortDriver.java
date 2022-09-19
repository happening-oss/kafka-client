package com.superology.kafka;

import java.io.*;
import java.util.*;

class PortDriver {
  public static void run(String[] args, Port port) {
    // Reading from the file descriptor 3, which is allocated by Elixir for input
    try (var input = new DataInputStream(new FileInputStream("/dev/fd/3"))) {
      ArrayList<Object> decodedArgs = new ArrayList<>();
      for (var arg : args)
        decodedArgs.add(decodeArg(arg));

      var output = PortOutput.start();

      var worker = PortWorker.start(port, output, decodedArgs.toArray());

      while (true) {
        var commandParts = nextCommand(input);

        var name = (String) commandParts[0];
        var commandArgs = new ArrayList<Object>();
        for (int i = 1; i < commandParts.length; i++)
          commandArgs.add(commandParts[i]);

        worker.command(new Port.Command(name, commandArgs.toArray()));
      }
    } catch (Exception e) {
      System.err.println(e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

  private static Object decodeArg(String encoded) throws Exception {
    var bytes = java.util.Base64.getDecoder().decode(encoded);
    return ErlangTermFormat.decode(bytes);
  }

  private static Object[] nextCommand(DataInputStream input) throws Exception {
    var length = readInt(input);
    var bytes = readBytes(input, length);
    return (Object[]) ErlangTermFormat.decode(bytes);
  }

  private static int readInt(DataInputStream input) throws IOException {
    var bytes = readBytes(input, 4);
    return new java.math.BigInteger(bytes).intValue();
  }

  private static byte[] readBytes(DataInputStream input, int length) throws IOException {
    var bytes = new byte[length];
    input.readFully(bytes);
    return bytes;
  }
}

interface Port {
  public void run(PortWorker worker, Object[] args, PortOutput output);

  record Command(String name, Object[] args) {
  }
}
