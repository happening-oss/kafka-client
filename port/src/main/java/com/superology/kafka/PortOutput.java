package com.superology.kafka;

import java.io.*;
import java.util.concurrent.*;
import com.ericsson.otp.erlang.*;

/*
 * The output thread of a port program. This thread is responsible for sending
 * messages to Elixir. Messages are sent as Erlang/Elixir terms.
 *
 * See {@link PortDriver} for details on port architecture. See {@link
 * ErlangTermFormat} for details on data encoding.
 */
final class PortOutput implements Runnable {
  public static PortOutput start() {
    var output = new PortOutput();

    // Using a daemon thread to ensure program termination if the main thread stops.
    var consumerThread = new Thread(output);
    consumerThread.setDaemon(true);
    consumerThread.start();

    return output;
  }

  private BlockingQueue<Message> messages;

  private PortOutput() {
    messages = new LinkedBlockingQueue<Message>();
  }

  public void emitCallResponse(Port.Command command, OtpErlangObject response) throws InterruptedException {
    emit(new OtpErlangTuple(new OtpErlangObject[] {
        new OtpErlangAtom("$kafka_consumer_response"),
        new OtpErlangBinary(command.ref().getBytes()),
        response }));
  }

  public void emit(OtpErlangObject message) throws InterruptedException {
    messages.put(new Message(message, System.nanoTime()));
  }

  @Override
  public void run() {
    // Writing to the file descriptor 4, which is allocated by Elixir for output
    try (var output = new DataOutputStream(new FileOutputStream("/dev/fd/4"))) {
      while (true) {
        var message = this.messages.take();

        // writing to the port is to some extent a blocking operation, so we measure it
        var sendingAt = System.nanoTime();
        notify(output, message.payload());
        var sentAt = System.nanoTime();

        // then we send the measurement to the output
        var duration = sentAt - message.startTime();
        notify(
            output,
            new OtpErlangTuple(new OtpErlangObject[] {
                new OtpErlangAtom("metrics"),
                new OtpErlangLong(sentAt - sendingAt),
                new OtpErlangLong(duration)
            }));
      }
    } catch (Exception e) {
      System.err.println(e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

  private void notify(DataOutputStream output, OtpErlangObject erlangTerm) throws IOException {
    byte[] payload = Erlang.encode(erlangTerm);

    // writing to the port

    // first four bytes are the length of the payload
    byte[] msgLength = java.nio.ByteBuffer.allocate(4).putInt(payload.length).array();
    output.write(msgLength);

    // then we write the actual payload
    output.write(payload);
  }

  record Message(OtpErlangObject payload, long startTime) {
  }
}
