package com.superology.kafka;

import java.io.*;
import java.util.*;
import org.apache.kafka.common.*;
import com.ericsson.otp.erlang.*;

public class ConsumerPort {
  public static void main(String[] args) {
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");

    try (var input = new DataInputStream(new FileInputStream("/dev/fd/3"))) {
      var poller = startPoller(args, ConsumerNotifier.start());

      while (true) {
        var command = nextCommand(input);
        handleCommand(poller, command);
      }
    } catch (Exception e) {
      System.err.println(e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

  private static ConsumerPoller startPoller(String[] args, ConsumerNotifier notifier)
      throws Exception, IOException, OtpErlangDecodeException, OtpErlangRangeException {
    var consumerProps = decodeProperties(args[0]);
    var topics = decodeTopics(args[1]);
    var pollerProps = decodeProperties(args[2]);
    var poller = ConsumerPoller.notifier(consumerProps, topics, pollerProps, notifier);
    return poller;
  }

  private static void handleCommand(ConsumerPoller poller, OtpErlangTuple command)
      throws OtpErlangRangeException, Exception {
    var tag = command.elementAt(0).toString();

    switch (tag) {
      case "ack":
        poller.addCommand(decodeAck(command));
        break;

      case "stop":
        poller.addCommand("stop");
        break;

      case "committed_offsets":
        poller.addCommand("committed_offsets");
        break;

      default:
        throw new Exception("unknown command " + tag);
    }
  }

  private static OtpErlangTuple nextCommand(DataInputStream input)
      throws IOException, OtpErlangDecodeException {
    var length = readInt(input);
    var bytes = readBytes(input, length);
    return (OtpErlangTuple) otpDecode(bytes);
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

  private static Ack decodeAck(OtpErlangTuple ack) throws OtpErlangRangeException {
    var topic = new String(((OtpErlangBinary) ack.elementAt(1)).binaryValue());
    var partitionNo = ((OtpErlangLong) ack.elementAt(2)).intValue();
    var partition = new TopicPartition(topic, partitionNo);
    var offset = ((OtpErlangLong) ack.elementAt(3)).longValue();
    return new Ack(partition, offset);
  }

  private static OtpErlangObject otpDecode(byte[] encoded) throws IOException, OtpErlangDecodeException {
    try (var inputStream = new OtpInputStream(encoded)) {
      return inputStream.read_any();
    }
  }
}
