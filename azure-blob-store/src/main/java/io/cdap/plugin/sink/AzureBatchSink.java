/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.sink;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.InvalidPluginConfigException;
import io.cdap.cdap.api.plugin.InvalidPluginProperty;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.validation.FormatContext;
import io.cdap.cdap.etl.api.validation.ValidatingOutputFormat;
import io.cdap.plugin.common.HiveSchemaConverter;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferenceBatchSink;
import io.cdap.plugin.common.ReferencePluginConfig;
import io.cdap.plugin.common.StructuredToAvroTransformer;
import io.cdap.plugin.common.StructuredToOrcTransformer;
import io.cdap.plugin.common.StructuredToTextTransformer;
import io.cdap.plugin.common.batch.JobUtils;
import io.cdap.plugin.common.batch.sink.SinkOutputFormatProvider;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("AzureBlobStoreSink")
@Description("Batch sink to read from Azure Blob Storage.")
public class AzureBatchSink extends ReferenceBatchSink<StructuredRecord, Object, Object> {
  private static final Logger LOG = LoggerFactory.getLogger(AzureBatchSink.class);
  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() {
  }.getType();
  private static final String AVRO = "avro";
  private static final String TEXT = "text";
  private static final String ORC = "orc";
  @SuppressWarnings("unused")
  private final AzureBlobSinkConfig config;
  private StructuredToAvroTransformer avroTransformer;
  private StructuredToTextTransformer textTransformer;
  private StructuredToOrcTransformer orcTransformer;

  public AzureBatchSink(AzureBlobSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    config.validate(pipelineConfigurer.getStageConfigurer().getInputSchema(), pipelineConfigurer.getStageConfigurer().getFailureCollector());
    if (!TEXT.equals(config.outputFormat)) {
      ValidatingOutputFormat validatingOutputFormat = getValidatingOutputFormat(pipelineConfigurer);
      FormatContext formatContext = new FormatContext(pipelineConfigurer.getStageConfigurer().getFailureCollector(), pipelineConfigurer.getStageConfigurer().getInputSchema());
      validateOutputFormatProvider(formatContext, config.outputFormat, validatingOutputFormat);
    }
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    String outputFormatClassName = null;
    if (!TEXT.equals(config.outputFormat)) {
      ValidatingOutputFormat validatingOutputFormat = getOutputFormatForRun(context);
      FormatContext formatContext = new FormatContext(collector, context.getInputSchema());
      validateOutputFormatProvider(formatContext, config.outputFormat, validatingOutputFormat);
      outputFormatClassName = validatingOutputFormat.getOutputFormatClassName();
    }
    collector.getOrThrowException();

    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();

    Map<String, String> properties = config.getFileSystemProperties();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }

    conf.set(FileOutputFormat.OUTDIR, config.path);
    job.setOutputValueClass(NullWritable.class);
    if (AVRO.equals(config.outputFormat)) {
      org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(config.getSchema().toString());
      AvroJob.setOutputKeySchema(job, avroSchema);
      context.addOutput(Output.of(config.referenceName, new SinkOutputFormatProvider(outputFormatClassName, conf)));
    } else if (ORC.equals(config.outputFormat)) {
      StringBuilder builder = new StringBuilder();
      HiveSchemaConverter.appendType(builder, config.getSchema());
      conf.set("orc.mapred.output.schema", builder.toString());
      context.addOutput(Output.of(config.referenceName, new SinkOutputFormatProvider(outputFormatClassName, conf)));
    } else {
      context.addOutput(Output.of(config.referenceName, new SinkOutputFormatProvider(TextOutputFormat.class.getName(), conf)));
    }

    LineageRecorder lineageRecorder = new LineageRecorder(context, config.referenceName);
    Schema schema = context.getInputSchema();
    if (schema != null) {
      lineageRecorder.createExternalDataset(schema);

      if (schema.getFields() != null) {
        lineageRecorder.recordWrite("Write", "Write to Azure Data Blob", schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList()));
      }
    }
  }

  protected ValidatingOutputFormat getValidatingOutputFormat(PipelineConfigurer pipelineConfigurer) {
    return pipelineConfigurer.usePlugin(ValidatingOutputFormat.PLUGIN_TYPE, config.outputFormat, config.outputFormat, config.getRawProperties());
  }

  private void validateOutputFormatProvider(FormatContext context, String format, @Nullable ValidatingOutputFormat validatingOutputFormat) {
    FailureCollector collector = context.getFailureCollector();
    if (validatingOutputFormat == null) {
      collector.addFailure(String.format("Could not find the '%s' output format plugin.", format), null).withPluginNotFound(format, format, ValidatingOutputFormat.PLUGIN_TYPE);
    } else {
      validatingOutputFormat.validate(context);
    }
  }

  protected ValidatingOutputFormat getOutputFormatForRun(BatchSinkContext context) throws InstantiationException {
    String fileFormat = config.outputFormat;
    try {
      return context.newPluginInstance(fileFormat);
    } catch (InvalidPluginConfigException e) {
      Set<String> properties = new HashSet<>(e.getMissingProperties());
      for (InvalidPluginProperty invalidProperty : e.getInvalidProperties()) {
        properties.add(invalidProperty.getName());
      }
      String errorMessage = String.format("Format '%s' cannot be used because properties %s were not provided or " + "were invalid when the pipeline was deployed. Set the format to a " + "different value, or re-create the pipeline with all required properties.", fileFormat, properties);
      throw new IllegalArgumentException(errorMessage, e);
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    if (AVRO.equals(config.outputFormat)) {
      avroTransformer = new StructuredToAvroTransformer(config.getSchema());
    } else if (ORC.equals(config.outputFormat)) {
      orcTransformer = new StructuredToOrcTransformer(config.getSchema());
    } else {
      textTransformer = new StructuredToTextTransformer(config.getFieldDelimiter(), config.getSchema());
    }
    super.initialize(context);
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<Object, Object>> emitter) throws Exception {
    if (TEXT.equals(config.outputFormat)) {
      emitter.emit(new KeyValue<>((Object) textTransformer.transform(input), (Object) NullWritable.get()));
    } else {
      emitter.emit(new KeyValue<>(NullWritable.get(), input));
    }
  }

  /**
   * Plugin config for {@link AzureBatchSink}.
   */
  public static class AzureBlobSinkConfig extends ReferencePluginConfig {
    protected static final String FILESYSTEM_PROPERTIES_DESCRIPTION = "A JSON string representing a map of properties "
      + "needed for the distributed file system.";
    private static final String PATH = "path";
    private static final String ACCOUNT = "account";
    private static final String AUTHENTICATION_METHOD = "authenticationMethod";
    private static final String STORAGE_ACCOUNT_KEY = "storageKey";
    private static final String SAS_TOKEN = "sasToken";
    private static final String CONTAINER = "container";
    private static final String STORAGE_ACCOUNT_KEY_AUTH_METHOD = "storageAccountKey";
    private static final String SAS_TOKEN_AUTH_METHOD = "sasToken";
    private static final String FILE_SYSTEM_PROPERTIES = "fileSystemProperties";
    private static final String OUTPUT_FORMAT = "outputFormat";
    private static final String SCHEMA = "schema";
    @Nullable
    @Description(FILESYSTEM_PROPERTIES_DESCRIPTION)
    @Macro
    public String fileSystemProperties;
    @Nullable
    @Description("Output schema of the JSON document. Required for avro output format. " + "If left empty for text output format, the schema of input records will be used." + "This must be a subset of the schema of input records. " + "Fields of type ARRAY, MAP, and RECORD are not supported with the text format. " + "Fields of type UNION are only supported if they represent a nullable type.")
    @Macro
    public String schema;
    @Nullable
    @Macro
    @Description("Field delimiter for text format output files. Defaults to tab.")
    public String fieldDelimiter;
    @Description("Path to file(s) to be read. If a directory is specified,terminate the path name with a '/'. " + "The path must start with `wasb://` or `wasbs://`.")
    @Macro
    private String path;
    @Description("The Microsoft Azure Storage account to use.")
    @Macro
    private String account;
    @Description("The authentication method to use to connect to Microsoft Azure. Can be either 'Storage Account Key'" + " or 'SAS Token'. Defaults to 'Storage Account Key'.")
    private String authenticationMethod;
    @Description("The storage key for the specified container on the specified Azure Storage account. Must be a " + "valid base64 encoded storage key provided by Microsoft Azure.")
    @Nullable
    @Macro
    private String storageKey;
    @Description("The SAS token to use to connect to the specified container. Required when authentication method " + "is set to 'SAS Token'.")
    @Nullable
    @Macro
    private String sasToken;
    @Description("The container to connect to. Required when authentication method is set to 'SAS Token'.")
    @Nullable
    @Macro
    private String container;
    @Description("The format of output files.")
    @Macro
    private String outputFormat;

    public AzureBlobSinkConfig(String referenceName) {
      super(referenceName);
    }

    protected void validate(Schema inputSchema, FailureCollector collector) {
      validate(collector);
      if (!containsMacro(SCHEMA)) {
        for (Schema.Field outputField : getSchema().getFields()) {
          String fieldName = outputField.getName();
          Schema.Field inputField = inputSchema.getField(outputField.getName());
          if (inputField == null) {
            collector.addFailure("Input schema does not contain the '" + fieldName + "' field.", null).withOutputSchemaField(fieldName);
          } else {
            if (TEXT.equals(outputFormat)) {
              Schema inputFieldSchema = inputField.getSchema();
              inputFieldSchema = inputFieldSchema.isNullable() ? inputFieldSchema.getNonNullable() : inputFieldSchema;
              Schema.Type inputType = inputFieldSchema.getType();
              switch (inputType) {
                case ARRAY:
                case MAP:
                case RECORD:
                  collector.addFailure(String.format("Field '%s' is of unexpected type '%s'.", fieldName, inputFieldSchema.getDisplayName()), "Provide type that is not array, map and record.").withInputSchemaField(fieldName);
              }
            }
          }
        }
      }
    }

    protected void validate(FailureCollector collector) {
      if (!containsMacro(PATH) && (!path.startsWith("wasb://") && !path.startsWith("wasbs://"))) {
        collector.addFailure("Path must start with wasb:// or wasbs:// for Windows Azure Blob Store " + "input files.", null).withConfigProperty(PATH);
      }
      if (!containsMacro(ACCOUNT) && !account.endsWith(".blob.core.windows.net")) {
        collector.addFailure("Account must end with '.blob.core.windows.net' for " + "Windows Azure Blob Store", null).withConfigProperty(ACCOUNT);
      }
      if (!(STORAGE_ACCOUNT_KEY_AUTH_METHOD.equalsIgnoreCase(authenticationMethod) || SAS_TOKEN_AUTH_METHOD.equalsIgnoreCase(authenticationMethod))) {
        collector.addFailure("Authentication method should be one of " + "'Storage Account Key' or 'SAS Token'", null).withConfigProperty(AUTHENTICATION_METHOD);
      }
      if (STORAGE_ACCOUNT_KEY_AUTH_METHOD.equalsIgnoreCase(authenticationMethod) && !containsMacro(STORAGE_ACCOUNT_KEY) && Strings.isNullOrEmpty(storageKey)) {
        collector.addFailure("Storage key must be provided when authentication method is set " + "to 'Storage Account Key'", null).withConfigProperty(STORAGE_ACCOUNT_KEY);
      }
      if (SAS_TOKEN_AUTH_METHOD.equalsIgnoreCase(authenticationMethod)) {
        if (!containsMacro(SAS_TOKEN) && Strings.isNullOrEmpty(sasToken)) {
          collector.addFailure("SAS token must be provided when authentication method is set to " + "'SAS Token'", null).withConfigProperty(SAS_TOKEN);
        }
        if (!containsMacro(CONTAINER) && Strings.isNullOrEmpty(container)) {
          collector.addFailure("Container must be provided when authentication method is set to " + "'SAS Token'", null).withConfigProperty(CONTAINER);
        }
      }
    }

    protected Map<String, String> getFileSystemProperties() {
      Map<String, String> properties = getProps();
      properties.put("fs.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem");
      properties.put("fs.wasb.impl.disable.cache", "true");
      properties.put("fs.wasbs.impl.disable.cache", "true");
      properties.put("fs.AbstractFileSystem.wasb.impl", "org.apache.hadoop.fs.azure.Wasb");
      if (STORAGE_ACCOUNT_KEY_AUTH_METHOD.equalsIgnoreCase(authenticationMethod)) {
        properties.put(String.format("fs.azure.account.key.%s", account), storageKey);
      } else if (SAS_TOKEN_AUTH_METHOD.equalsIgnoreCase(authenticationMethod)) {
        properties.put(String.format("fs.azure.sas.%s.%s", container, account), sasToken);
      }
      return properties;
    }

    @Nullable
    public Schema getSchema() {
      if (schema == null) {
        return null;
      }
      try {
        return Schema.parseJson(schema);
      } catch (IOException e) {
        throw new IllegalArgumentException("Unable to parse output schema.");
      }
    }

    public String getFieldDelimiter() {
      return fieldDelimiter == null ? "\t" : fieldDelimiter;
    }

    protected Map<String, String> getProps() {
      if (fileSystemProperties == null) {
        return new HashMap<>();
      }
      try {
        return GSON.fromJson(fileSystemProperties, MAP_STRING_STRING_TYPE);
      } catch (Exception e) {
        throw new IllegalArgumentException("Unable to parse fileSystemProperties: " + e.getMessage());
      }
    }
  }
}
