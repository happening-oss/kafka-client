package com.happening.kafka.port;

/*
 * Specifies an interface which must be implemented by the concrete port
 * implementations.
 *
 * See {@link com.happening.kafka.admin.Main} for an example.
 */
public interface Port {
    // Invoked in the worker thread to run main port loop. After the function
    // returns, the program will stop, using the returned value as the exit code.
    public int run(Worker worker, Output output, Object[] args) throws Exception;

    record Command(String name, Object[] args, String ref) {
    }
}
