package com.superology.kafka.port;

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
 * (see {@link Erlang}). These messages are forwarded to the worker thread
 * (powered by {@link Worker}), which where the actual concrete logic of the
 * port is running. Finally, the output thread ({@link Output}) is responsible
 * for sending messages to Erlang/Elixir.
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
public class Driver {
    public static void run(String[] args, Port port) {
        // Reading from the file descriptor 3, which is allocated by Elixir for input
        try (var input = new DataInputStream(new FileInputStream("/dev/fd/3"))) {
            ArrayList<Object> decodedArgs = new ArrayList<>();
            for (var arg : args)
                decodedArgs.add(decodeArg(arg));

            var output = Output.start();
            var worker = Worker.start(port, output, decodedArgs.toArray());

            try {
                while (true) {
                    var command = takeCommand(input);
                    worker.sendCommand(command);
                }
            } catch (EOFException e) {
                // port owner has terminated

                // try to stop gracefully by asking the worker to stop
                worker.sendCommand(new Port.Command("stop", new Object[]{}, null));

                // if the worker doesn't stop in 5 seconds, exit
                Thread.sleep(5000);
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static Object decodeArg(String encoded) throws Exception {
        var bytes = java.util.Base64.getDecoder().decode(encoded);
        return Erlang.decode(bytes);
    }

    private static Port.Command takeCommand(DataInputStream input) throws Exception {
        var length = readInt(input);
        var bytes = readBytes(input, length);
        var commandTuple = (Object[]) Erlang.decode(bytes);
        return buildCommand(commandTuple);
    }

    private static Port.Command buildCommand(Object[] commandTuple) {
        @SuppressWarnings("unchecked")
        var args = (Collection<Object>) commandTuple[1];
        return new Port.Command((String) commandTuple[0], args.toArray(), (String) commandTuple[2]);
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
