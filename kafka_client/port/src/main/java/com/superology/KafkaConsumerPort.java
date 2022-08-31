package com.superology;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.ericsson.otp.erlang.*;

public class KafkaConsumerPort {
  public static void main(String[] args) {
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");

    System.out.println("kafka consumer port started");

    try (var input = new DataInputStream(new FileInputStream("/dev/fd/3"));
        var consumer = consumer(args[0])) {
      var output = KafkaConsumerOutput.start();

      var topics = new ArrayList<String>();
      var topicBytes = java.util.Base64.getDecoder().decode(args[1]);
      for (var topic : (OtpErlangList) otpDecode(topicBytes))
        topics.add(new String(((OtpErlangBinary) topic).binaryValue()));
      System.out.println("subscribing to: " + topics.toString());
      consumer.subscribe(topics);

      while (true) {
        var length = readInt(input);
        var bytes = readBytes(input, length);
        var tuple = (OtpErlangTuple) otpDecode(bytes);

        switch (tuple.elementAt(0).toString()) {
          case "poll":
            var duration = ((OtpErlangLong) tuple.elementAt(1)).intValue();
            var records = consumer.poll(java.time.Duration.ofMillis(duration));
            output.write(records);
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

  private static KafkaConsumer<String, byte[]> consumer(String encodedParams)
      throws Exception, IOException, OtpErlangDecodeException, OtpErlangRangeException {
    var consumerProps = new java.util.Properties();
    var paramBytes = java.util.Base64.getDecoder().decode(encodedParams);
    var params = (OtpErlangMap) otpDecode(paramBytes);

    for (var foo : params.entrySet()) {
      var key = new String(((OtpErlangBinary) foo.getKey()).binaryValue());
      Object value;

      if (foo.getValue() instanceof OtpErlangBinary)
        value = new String(((OtpErlangBinary) foo.getValue()).binaryValue());
      else if (foo.getValue() instanceof OtpErlangLong)
        value = ((OtpErlangLong) foo.getValue()).intValue();
      else
        throw new Exception("unknown type " + foo.getValue().getClass().toString());

      consumerProps.put(key, value);
    }

    return new KafkaConsumer<String, byte[]>(consumerProps);
  }

  private static OtpErlangObject otpDecode(byte[] encoded) throws IOException, OtpErlangDecodeException {
    try (var inputStream = new OtpInputStream(encoded)) {
      return inputStream.read_any();
    }
  }
}
