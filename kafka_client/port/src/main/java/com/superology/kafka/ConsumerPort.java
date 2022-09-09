package com.superology.kafka;

import java.io.*;
import java.util.*;
import org.apache.kafka.common.*;
import com.ericsson.otp.erlang.*;

public class ConsumerPort {
  public static void main(String[] args) {
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");

    try (var input = new DataInputStream(new FileInputStream("/dev/fd/3"))) {
      var output = ConsumerOutput.start();
      var poller = startPoller(args, output);

      while (true) {
        var message = readMessage(input);
        handleMessage(poller, message);
      }
    } catch (Exception e) {
      System.err.println(e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

  private static ConsumerPoller startPoller(String[] args, ConsumerOutput output)
      throws Exception, IOException, OtpErlangDecodeException, OtpErlangRangeException {
    var consumerProps = decodeProperties(args[0]);
    var topics = decodeTopics(args[1]);
    var pollerProps = decodeProperties(args[2]);
    var poller = ConsumerPoller.start(consumerProps, topics, pollerProps, output);
    return poller;
  }

  private static void handleMessage(ConsumerPoller poller, OtpErlangTuple message)
      throws OtpErlangRangeException, Exception {
    var tag = message.elementAt(0).toString();

    switch (tag) {
      case "ack":
        poller.addMessage(decodeAck(message));
        break;

      case "stop":
        poller.addMessage("stop");
        break;

      case "committed_offsets":
        poller.addMessage("committed_offsets");
        break;

      default:
        throw new Exception("unknown message " + tag);
    }
  }

  private static OtpErlangTuple readMessage(DataInputStream input)
      throws IOException, OtpErlangDecodeException {
    var length = readInt(input);
    var messageBytes = readBytes(input, length);
    var message = (OtpErlangTuple) otpDecode(messageBytes);
    return message;
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
      var value = otpObjectToJava(param.getValue());

      if (value != null)
        consumerProps.put(key, value);
    }

    return consumerProps;
  }

  private static Object otpObjectToJava(OtpErlangObject value) throws OtpErlangRangeException, Exception {
    if (value instanceof OtpErlangBinary)
      return new String(((OtpErlangBinary) value).binaryValue());
    else if (value instanceof OtpErlangLong)
      return ((OtpErlangLong) value).intValue();
    else if (value instanceof OtpErlangAtom) {
      var atomValue = ((OtpErlangAtom) value).atomValue();
      switch (atomValue) {
        case "true":
        case "false":
          return Boolean.parseBoolean(atomValue);

        case "nil":
          return null;
      }
    }

    throw new Exception("error converting to java object " + value);
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
