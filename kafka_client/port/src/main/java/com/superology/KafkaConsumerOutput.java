package com.superology;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.consumer.ConsumerRecords;

import com.ericsson.otp.erlang.*;

final class KafkaConsumerOutput implements Runnable {
  public static KafkaConsumerOutput start() {
    var output = new KafkaConsumerOutput();

    var consumerThread = new Thread(output);
    consumerThread.setDaemon(true);
    consumerThread.start();

    return output;
  }

  private BlockingQueue<Object> outputs;

  private KafkaConsumerOutput() {
    this.outputs = new LinkedBlockingQueue<Object>();
  }

  public void write(Object message)
      throws InterruptedException {
    this.outputs.put(message);
  }

  @Override
  public void run() {
    try (var output = new DataOutputStream(new FileOutputStream("/dev/fd/4"))) {

      while (true) {
        var message = this.outputs.take();

        if (message instanceof ConsumerRecords) {
          @SuppressWarnings("unchecked")
          var records = (ConsumerRecords<String, byte[]>) message;
          for (var record : records) {
            var encodedMessage = new OtpErlangTuple(new OtpErlangObject[] {
                new OtpErlangAtom("record"),
                new OtpErlangBinary(record.topic().getBytes()),
                new OtpErlangInt(record.partition()),
                new OtpErlangLong(record.offset()),
                new OtpErlangLong(record.timestamp()),
                new OtpErlangBinary(record.value())
            });

            write(output, encodedMessage);
          }
        } else if (message instanceof OtpErlangObject) {
          write(output, (OtpErlangObject) message);
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
