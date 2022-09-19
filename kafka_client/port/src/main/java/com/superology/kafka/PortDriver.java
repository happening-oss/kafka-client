package com.superology.kafka;

import java.io.*;
import java.util.*;
import com.ericsson.otp.erlang.*;

class PortDriver {
  public static void run(String[] args, Port port) {
    // Reading from the file descriptor 3, which is allocated by Elixir for input
    try (var input = new DataInputStream(new FileInputStream("/dev/fd/3"))) {
      ArrayList<Object> decodedArgs = new ArrayList<>();
      for (var arg : args)
        decodedArgs.add(decodeArg(arg));

      port.initialize(decodedArgs.toArray());

      while (true) {
        var commandParts = nextCommand(input);

        var name = (String) commandParts[0];
        var commandArgs = new ArrayList<Object>();
        for (int i = 1; i < commandParts.length; i++)
          commandArgs.add(commandParts[i]);

        port.handleCommand(name, commandArgs.toArray());
      }
    } catch (Exception e) {
      System.err.println(e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

  private static Object decodeArg(String encoded) throws Exception {
    var bytes = java.util.Base64.getDecoder().decode(encoded);
    var otpDecoded = otpDecode(bytes);
    return fromErlang(otpDecoded);
  }

  private static Object[] nextCommand(DataInputStream input) throws Exception {
    var length = readInt(input);
    var bytes = readBytes(input, length);
    return (Object[]) fromErlang((OtpErlangTuple) otpDecode(bytes));
  }

  private static int readInt(DataInputStream input) throws IOException {
    var bytes = readBytes(input, 4);
    return new java.math.BigInteger(bytes).intValue();
  }

  private static byte[] readBytes(DataInputStream input, int length) throws IOException {
    var bytes = new byte[length];
    input.readFully(bytes);
    return bytes;
  }

  private static OtpErlangObject otpDecode(byte[] encoded) throws IOException, OtpErlangDecodeException {
    try (var inputStream = new OtpInputStream(encoded)) {
      return inputStream.read_any();
    }
  }

  private static Object fromErlang(OtpErlangObject value) throws Exception {
    if (value instanceof OtpErlangList)
      return fromErlangList((OtpErlangList) value);
    else if (value instanceof OtpErlangTuple)
      return fromErlangList(Arrays.asList(((OtpErlangTuple) value).elements())).toArray();
    else if (value instanceof OtpErlangMap)
      return fromErlangMap((OtpErlangMap) value);
    else if (value instanceof OtpErlangBinary)
      return new String(((OtpErlangBinary) value).binaryValue());
    else if (value instanceof OtpErlangLong) {
      var number = ((OtpErlangLong) value).longValue();
      if (number >= Integer.MIN_VALUE && number <= Integer.MAX_VALUE)
        return (int) number;
      else
        return number;
    } else if (value instanceof OtpErlangAtom) {
      var atomValue = ((OtpErlangAtom) value).atomValue();
      switch (atomValue) {
        case "true":
        case "false":
          return Boolean.parseBoolean(atomValue);

        case "nil":
          return null;

        default:
          return atomValue;
      }
    }

    throw new Exception("error converting " + value.getClass() + " to java object: " + value);
  }

  private static Collection<Object> fromErlangList(Iterable<OtpErlangObject> objects) throws Exception {
    var result = new ArrayList<Object>();

    for (var object : objects)
      result.add(fromErlang(object));

    return result;
  }

  private static Map<Object, Object> fromErlangMap(OtpErlangMap otpMap) throws Exception {
    var map = new HashMap<Object, Object>();

    for (var param : otpMap.entrySet()) {
      var key = fromErlang(param.getKey());
      var value = fromErlang(param.getValue());
      map.put(key, value);
    }

    return map;
  }
}

interface Port {
  public void initialize(Object[] args);

  public void handleCommand(String name, Object[] args);
}
