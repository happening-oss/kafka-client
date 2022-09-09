package com.superology.kafka;

import java.io.*;
import java.util.concurrent.*;
import com.ericsson.otp.erlang.*;

final class ConsumerNotifier implements Runnable {
  public static ConsumerNotifier start() {
    var output = new ConsumerNotifier();

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
    try (var output = new DataOutputStream(new FileOutputStream("/dev/fd/4"))) {
      while (true) {
        var message = this.notifications.take();

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

  record Notification(OtpErlangObject payload, Long startTime) {
  }
}
