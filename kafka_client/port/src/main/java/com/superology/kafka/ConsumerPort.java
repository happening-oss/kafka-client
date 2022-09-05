package com.superology.kafka;

import java.io.*;
import java.util.*;
import org.apache.kafka.common.*;
import com.ericsson.otp.erlang.*;

public class ConsumerPort {
  public static void main(String[] args) {
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");

    try (var input = new DataInputStream(new FileInputStream("/dev/fd/3"))) {
      var consumerProps = decodeProperties(args[0]);
      var topics = decodeTopics(args[1]);
      var pollerProps = decodeProperties(args[2]);
      var output = ConsumerOutput.start();
      var poller = ConsumerPoller.start(consumerProps, topics, pollerProps, output);

      while (true) {
        var length = readInt(input);
        var messageBytes = readBytes(input, length);
        var message = (OtpErlangTuple) otpDecode(messageBytes);

        switch (message.elementAt(0).toString()) {
          case "ack":
            poller.addMessage(decodeAck(message));
            break;

          case "stop":
            poller.addMessage("stop");
            break;

          case "committed_offsets":
            poller.addMessage("committed_offsets");
            break;
        }
      }
    } catch (java.io.EOFException e) {
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

  private static Properties decodeProperties(String encoded)
      throws Exception, IOException, OtpErlangDecodeException, OtpErlangRangeException {
    var consumerProps = new Properties();
    var paramBytes = java.util.Base64.getDecoder().decode(encoded);
    var params = (OtpErlangMap) otpDecode(paramBytes);

    for (var param : params.entrySet()) {
      var key = new String(((OtpErlangBinary) param.getKey()).binaryValue());
      Object value = param.getValue();

      if (value instanceof OtpErlangBinary)
        value = new String(((OtpErlangBinary) value).binaryValue());
      else if (value instanceof OtpErlangLong)
        value = ((OtpErlangLong) value).intValue();
      else if (value instanceof OtpErlangAtom) {
        var atomValue = ((OtpErlangAtom) value).atomValue();
        if (atomValue.equals("true"))
          value = true;
        else if (atomValue.equals("false"))
          value = false;
      } else
        throw new Exception("unknown type " + param.getValue().getClass().toString());

      consumerProps.put(key, value);
    }

    return consumerProps;
  }

  private static Collection<String> decodeTopics(String encoded) throws IOException, OtpErlangDecodeException {
    var topics = new ArrayList<String>();
    var topicBytes = java.util.Base64.getDecoder().decode(encoded);
    for (var topic : (OtpErlangList) otpDecode(topicBytes))
      topics.add(new String(((OtpErlangBinary) topic).binaryValue()));
    return topics;
  }

  private static Ack decodeAck(OtpErlangTuple message) throws OtpErlangRangeException {
    var topic = new String(((OtpErlangBinary) message.elementAt(1)).binaryValue());
    var partitionNo = ((OtpErlangLong) message.elementAt(2)).intValue();
    var partition = new TopicPartition(topic, partitionNo);
    var offset = ((OtpErlangLong) message.elementAt(3)).longValue();
    return new Ack(partition, offset);
  }

  private static OtpErlangObject otpDecode(byte[] encoded) throws IOException, OtpErlangDecodeException {
    try (var inputStream = new OtpInputStream(encoded)) {
      return inputStream.read_any();
    }
  }
}
