## Getting started

To get started, follow instruction from [README](README.md) to install all the tools you will need.

## IDE setup

### IntelliJ IDEA setup

For IntelliJ, you will need our custom code style. To set it up, download [happening-settings.zip](https://github.com/happening-oss/kafka-client/files/12882697/happening-settings.zip) file and use `Import Settings` action in IntelliJ to import it. After that, go to `Settings -> Editor -> Code Style` and select `Happening-Java` scheme.

### VSCode setup

For VSCode, there's already project-specific configuration set up in `.vscode` folder. For this to work, you will also need [Java Language Support](https://marketplace.visualstudio.com/items?itemName=redhat.java) extension or you can follow [an official guide](https://code.visualstudio.com/docs/languages/java) and install the whole pack suggested by Microsoft.

### Other editors and formatting

Since we are using [Spotless](https://github.com/diffplug/spotless) with Eclipse XML configuration you can simply run `mvn spotless:check` to verify that the formatting is correct or `mvn spotless:apply` to fix it.
