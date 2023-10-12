package com.superology.kafka.producer;

import java.util.*;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Header;

import com.ericsson.otp.erlang.*;
import com.superology.kafka.port.*;

public class Main implements Port {
    public static void main(String[] args) {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");
        Driver.run(args, new Main());
    }

    private Map<String, Handler> dispatchMap = Map.ofEntries(
            Map.entry("partitions_for", this::partitionsFor),
            Map.entry("metrics", this::metrics),
            Map.entry("stop", this::stop),
            Map.entry("send", this::send)
    );

    @Override
    public int run(Worker worker, Output output, Object[] args) throws Exception {
        @SuppressWarnings("unchecked")
        var props = mapToProperties((Map<Object, Object>) args[0]);

        try (var producer = new Producer(props)) {
            while (true) {
                var command = worker.take();
                var exitCode = dispatchMap.get(command.name()).handle(producer, command, output);
                if (exitCode != null)
                    return exitCode;
            }
        }
    }

    private Integer metrics(Producer producer, Port.Command command, Output output) throws InterruptedException {
        OtpErlangObject response;
        try {
            var map = Erlang.toMap(
                    producer.metrics(),
                    entry -> {
                        OtpErlangObject value;
                        var metricsValue = entry.getValue().metricValue();
                        if (metricsValue instanceof java.lang.Double val) {
                            value = Double.isNaN(val) ? Erlang.nil() : new OtpErlangDouble(val);
                        } else if (metricsValue instanceof java.lang.Long val) {
                            value = new OtpErlangLong(val);
                        } else {
                            value = new OtpErlangBinary(metricsValue.toString().getBytes());
                        }
                        return Erlang.mapEntry(new OtpErlangBinary(entry.getKey().name().getBytes()), value);
                    }
            );
            response = Erlang.ok(map);
        } catch (Exception e) {
            response = Erlang.error(new OtpErlangBinary(e.getCause().getMessage().getBytes()));
        }

        output.emitCallResponse(command, response);

        return null;
    }

    private Integer partitionsFor(
            Producer producer, Port.Command command, Output output
    ) throws InterruptedException, KafkaException {
        var topic = (String) command.args()[0];

        OtpErlangObject response;
        try {
            var list = Erlang.toList(
                    producer.partitionsFor(topic),
                    entry -> {
                        return new OtpErlangInt(entry.partition());
                    }
            );
            response = Erlang.ok(list);
        } catch (Exception e) {
            response = Erlang.error(new OtpErlangBinary(e.getCause().getMessage().getBytes()));
        }

        output.emitCallResponse(command, response);

        return null;
    }

    private Integer stop(Producer producer, Port.Command command, Output output) {
        producer.flush();
        return 0;
    }

    private Integer send(
            Producer producer, Port.Command command, Output output
    ) throws InterruptedException, ExecutionException {
        @SuppressWarnings("unchecked")
        var record = (Map<String, Object>) command.args()[0];

        var headers = new LinkedList<Header>();
        for (@SuppressWarnings("unchecked")
        var header : (Collection<Object[]>) record.get("headers")) {
            headers.add(new Header() {
                public String key() {
                    return (String) header[0];
                }

                public byte[] value() {
                    return (byte[]) header[1];
                }
            });
        }

        Callback callback = null;
        var callbackId = (byte[]) command.args()[1];
        if (callbackId != null) {
            callback = new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    OtpErlangObject payload;

                    if (e != null) {
                        if (e instanceof TimeoutException) {
                            payload = Erlang.error(new OtpErlangAtom("timeout"));
                        } else {
                            payload = Erlang.error(new OtpErlangBinary(e.getMessage().getBytes()));
                        }
                    } else {
                        payload = Erlang.ok(
                                new OtpErlangInt(metadata.partition()),
                                new OtpErlangLong(metadata.offset()),
                                new OtpErlangLong(metadata.timestamp())
                        );
                    }
                    try {
                        output.emit(
                                Erlang.tuple(
                                        new OtpErlangAtom("on_completion"),
                                        new OtpErlangBinary(callbackId),
                                        payload
                                )
                        );
                    } catch (InterruptedException ie) {
                        throw new org.apache.kafka.common.errors.InterruptException(ie);
                    }
                }
            };
        }

        producer.send(
                new ProducerRecord<>(
                        (String) record.get("topic"),
                        (Integer) record.get("partition"),
                        (Long) record.get("timestamp"),
                        (byte[]) record.get("key"),
                        (byte[]) record.get("value"),
                        headers
                ),
                callback
        );

        return null;
    }

    private Properties mapToProperties(Map<Object, Object> map) {
        // need to remove nulls, because Properties doesn't support them
        map.values().removeAll(Collections.singleton(null));
        var result = new Properties();
        result.putAll(map);
        return result;
    }

    @FunctionalInterface
    interface Handler {
        Integer handle(Producer producer, Port.Command command, Output output) throws Exception;
    }
}

final class Producer extends KafkaProducer<byte[], byte[]> {
    public Producer(Properties properties) {
        super(properties);
    }
}
