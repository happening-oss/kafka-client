package com.superology.kafka;

import java.io.*;
import java.util.*;
import com.ericsson.otp.erlang.*;

public class ErlangTermFormat {
  // This is a Java version of `:erlang.term_to_binary`, powered by JInterface
  // (https://www.erlang.org/doc/apps/jinterface/java/com/ericsson/otp/erlang/package-summary.html)
  public static byte[] encode(OtpErlangObject encodedMessage) throws IOException {
    try (var otpOutStream = new OtpOutputStream(encodedMessage);
        var byteStream = new java.io.ByteArrayOutputStream()) {
      // OtpOutputStream.writeToAndFlush produces the binary without the leading
      // version number byte (131), so we need to include it ourselves.
      //
      // The version number is not included because JInterface is developed for
      // exchanging messages between Erlang nodes, and in this mode of operation the
      // leading version byte is not included.
      //
      // See https://www.erlang.org/doc/apps/erts/erl_ext_dist.html for details

      byteStream.write(131);
      otpOutStream.writeToAndFlush(byteStream);

      return byteStream.toByteArray();
    }
  }

  public static Object decode(byte[] encoded) throws Exception {
    try (var inputStream = new OtpInputStream(encoded)) {
      return fromErlang(inputStream.read_any());
    }
  }

  private static Object fromErlang(OtpErlangObject value) throws Exception {
    if (value instanceof OtpErlangList)
      return ErlangTermFormat.fromErlangList((OtpErlangList) value);
    else if (value instanceof OtpErlangTuple)
      return ErlangTermFormat.fromErlangList(Arrays.asList(((OtpErlangTuple) value).elements())).toArray();
    else if (value instanceof OtpErlangMap)
      return ErlangTermFormat.fromErlangMap((OtpErlangMap) value);
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
