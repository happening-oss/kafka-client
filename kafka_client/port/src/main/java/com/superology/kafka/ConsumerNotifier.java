package com.superology.kafka;

import java.io.*;
import java.util.concurrent.*;
import com.ericsson.otp.erlang.*;

final class ConsumerNotifier implements Runnable {
  public static ConsumerNotifier start() {
    var output = new ConsumerNotifier();

    // Using a daemon thread to ensure program termination if the main thread stops.
    var consumerThread = new Thread(output);
    consumerThread.setDaemon(true);
    consumerThread.start();

    return output;
  }

  private BlockingQueue<Notification> notifications;

  private ConsumerNotifier() {
    notifications = new LinkedBlockingQueue<Notification>();
  }

  public void emit(OtpErlangObject notification) throws InterruptedException {
    notifications.put(new Notification(notification, System.nanoTime()));
  }

  @Override
  public void run() {
    // Writing to the file descriptor 4, which is allocated by Elixir for output
    try (var output = new DataOutputStream(new FileOutputStream("/dev/fd/4"))) {
      while (true) {
        var message = this.notifications.take();

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
    byte[] payload = termToBinary(erlangTerm);

    // writing to the port

    // first four bytes are the length of the payload
    byte[] msgLength = java.nio.ByteBuffer.allocate(4).putInt(payload.length).array();
    output.write(msgLength);

    // then we write the actual payload
    output.write(payload);
  }

  // This is a Java version of `:erlang.term_to_binary`, powered by JInterface
  // (https://www.erlang.org/doc/apps/jinterface/java/com/ericsson/otp/erlang/package-summary.html)
  private byte[] termToBinary(OtpErlangObject encodedMessage) throws IOException {
    try (var otpOutStream = new OtpOutputStream(encodedMessage);
        var byteStream = new java.io.ByteArrayOutputStream()) {
      // OtpOutputStream.writeToAndFlush produces the binary without the leading
      // version number byte (131), so we need to include it ourselves.
      //
      // The version number is not included because JInterface is developed for
      // exchanging messages between Erlang nodes, and in this mode of operation the
      // leading version byte is not included.
      //
      // See https://www.erlang.org/doc/apps/erts/erl_ext_dist.html for details

      byteStream.write(131);
      otpOutStream.writeToAndFlush(byteStream);

      return byteStream.toByteArray();
    }
  }

  record Notification(OtpErlangObject payload, long startTime) {
  }
}
