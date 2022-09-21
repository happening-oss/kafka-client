package com.superology.kafka;

import java.io.*;
import java.util.*;

/*
 * This class implements the generic behaviour of a Java port. The concrete
 * ports need to implement the {@link Port} interface to provide the specific
 * behaviour of the port.
 *
 * This class splits the port into three threads: the main (input) thread, the
 * worker, and the output. The main thread (powered by this class) accepts
 * incoming messages from Erlang/Elixir, which are sent as encoded Erlang terms
 * (see {@link ErlangTermFormat}). These messages are forwarded to the worker
 * thread (powered by {@link PortWorker}), which where the actual concrete logic
 * of the port is running. Finally, the output thread ({@link PortOutput}) is
 * responsible for sendin messages to Erlang/Elixir.
 *
 * The reason for separating the input thread from the worker is to support
 * immediate termination even when the worker is busy. When Elixir closes the
 * port, the input thread will detect an EOF, and it will immediately halt the
 * entire program.
 *
 * The reason for separating the output thread from the worker is to improve
 * the throughput. Sending the data to Elixir is an I/O operation which might take
 * longer for very large messages. We don't want to pause the worker logic while
 * this is happening, and so these two jobs are running in separate threads.
 */
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
        var command = takeCommand(input);
        worker.sendCommand(command);
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

  private static Port.Command takeCommand(DataInputStream input) throws Exception {
    var length = readInt(input);
    var bytes = readBytes(input, length);
    var commandTuple = (Object[]) ErlangTermFormat.decode(bytes);
    return buildCommand(commandTuple);
  }

  @SuppressWarnings("unchecked")
  private static Port.Command buildCommand(Object[] commandTuple) {
    return new Port.Command(
        (String) commandTuple[0],
        ((Collection<Object>) commandTuple[1]).toArray(),
        (String) commandTuple[2]);
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

/*
 * Specifies an interface which must be implemented by the concrete port
 * implementations.
 *
 * See {@link ConsumerPort} for an example.
 */
interface Port {
  // Invoked in the worker thread to run main port loop.
  public void run(PortWorker worker, PortOutput output, Object[] args);

  record Command(String name, Object[] args, String ref) {
  }
}
