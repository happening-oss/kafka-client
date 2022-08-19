package com.superology;

import java.io.DataOutputStream;
import java.io.FileOutputStream;

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

  private BlockingQueue<ConsumerRecords<String, byte[]>> records;

  private KafkaConsumerOutput() {
    this.records = new LinkedBlockingQueue<ConsumerRecords<String, byte[]>>();
  }

  public void write(ConsumerRecords<String, byte[]> newRecords)
      throws InterruptedException {
    if (!newRecords.isEmpty())
      this.records.put(newRecords);
  }

  @Override
  public void run() {
    try (var output = new DataOutputStream(new FileOutputStream("/dev/fd/4"))) {
      while (true) {
        var records = this.records.take();
        for (var record : records) {
          var message = new OtpErlangTuple(new OtpErlangObject[] {
              new OtpErlangAtom("record"),
              new OtpErlangBinary(record.topic().getBytes()),
              new OtpErlangInt(record.partition()),
              new OtpErlangLong(record.offset()),
              new OtpErlangLong(record.timestamp()),
              new OtpErlangBinary(record.value())
          });

          try (var otpOutStream = new OtpOutputStream(message);
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
    } catch (Exception e) {
      System.err.println(e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }
}
