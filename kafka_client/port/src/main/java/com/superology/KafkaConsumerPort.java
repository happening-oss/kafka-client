package com.superology;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.ericsson.otp.erlang.*;

public class KafkaConsumerPort {
  public static void main(String[] args) {
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");

    System.out.println("kafka consumer port started");

    try (var input = new DataInputStream(new FileInputStream("/dev/fd/3"));
        var consumer = consumer()) {
      var output = KafkaConsumerOutput.start();

      var topics = java.util.Arrays.asList("mytopic");
      consumer.subscribe(topics);

      while (true) {
        var length = readInt(input);
        var bytes = readBytes(input, length);

        try (var inputStream = new OtpInputStream(bytes, 0)) {
          var tuple = (OtpErlangTuple) inputStream.read_any();
          switch (tuple.elementAt(0).toString()) {
            case "poll":
              var duration = ((OtpErlangLong) tuple.elementAt(1)).intValue();
              var records = consumer.poll(java.time.Duration.ofMillis(duration));
              output.write(records);
          }
        }
      }
    } catch (java.io.EOFException e) {
      System.out.println("kafka consumer port stopped");
      System.exit(0);
    } catch (Exception e) {
      System.err.println(e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

  private static int readInt(DataInputStream input)
      throws IOException {
    var bytes = readBytes(input, 4);
    return new java.math.BigInteger(bytes).intValue();
  }

  private static byte[] readBytes(DataInputStream input, int length)
      throws IOException {
    var bytes = new byte[length];
    input.readFully(bytes);
    return bytes;
  }

  private static KafkaConsumer<String, byte[]> consumer() {
    var consumerProps = new java.util.Properties();
    consumerProps.put("bootstrap.servers", "localhost:9092");
    consumerProps.put("group.id", "test");
    consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    consumerProps.put("max.poll.interval.ms", 1000);

    return new KafkaConsumer<String, byte[]>(consumerProps);
  }
}
