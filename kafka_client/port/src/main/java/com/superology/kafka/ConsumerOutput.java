package com.superology.kafka;

import java.io.*;
import java.util.concurrent.*;
import com.ericsson.otp.erlang.*;

final class ConsumerOutput implements Runnable {
  public static ConsumerOutput start() {
    var output = new ConsumerOutput();

    var consumerThread = new Thread(output);
    consumerThread.setDaemon(true);
    consumerThread.start();

    return output;
  }

  private BlockingQueue<Message> messages;

  private ConsumerOutput() {
    messages = new LinkedBlockingQueue<Message>();
  }

  public void write(OtpErlangObject payload)
      throws InterruptedException {
    messages.put(new Message(payload, System.nanoTime()));
  }

  @Override
  public void run() {
    try (var output = new DataOutputStream(new FileOutputStream("/dev/fd/4"))) {
      while (true) {
        var message = this.messages.take();

        var sendingAt = System.nanoTime();
        write(output, message.payload());
        var sentAt = System.nanoTime();

        if (message.startTime() != null) {
          var duration = sentAt - message.startTime();
          write(
              output,
              new OtpErlangTuple(new OtpErlangObject[] {
                  new OtpErlangAtom("metrics"),
                  new OtpErlangLong(sentAt - sendingAt),
                  new OtpErlangLong(duration)
              }));
        }
      }
    } catch (Exception e) {
      System.err.println(e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

  private void write(DataOutputStream output, OtpErlangObject encodedMessage) throws IOException {
    try (var otpOutStream = new OtpOutputStream(encodedMessage);
        var byteStream = new java.io.ByteArrayOutputStream()) {
      byteStream.write(131);
      otpOutStream.writeToAndFlush(byteStream);

      byte[] bytes = byteStream.toByteArray();
      byte[] msgLength = java.nio.ByteBuffer.allocate(4).putInt(bytes.length).array();

      output.write(msgLength);
      output.write(bytes);
    }
  }
}

record Message(OtpErlangObject payload, Long startTime) {
}
