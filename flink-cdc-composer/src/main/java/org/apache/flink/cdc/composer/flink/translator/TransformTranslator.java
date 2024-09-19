package org.apache.flink.cdc.composer.flink.translator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.composer.definition.ModelDef;
import org.apache.flink.cdc.composer.definition.TransformDef;
import org.apache.flink.cdc.composer.definition.UdfDef;
import org.apache.flink.cdc.runtime.operators.transform.PostTransformOperator;
import org.apache.flink.cdc.runtime.operators.transform.PreTransformOperator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Translator used to build {@link PreTransformOperator} and {@link PostTransformOperator} for event
 * transform.
 */
public class TransformTranslator {

    private static final String MODEL_UDF_CLASSPATH =
            "org.apache.flink.cdc.runtime.operators.model.ModelUdf";
    private static final String PARAM_SEPARATOR = ":::";

    public DataStream<Event> translatePreTransform(
            DataStream<Event> input,
            List<TransformDef> transforms,
            List<UdfDef> udfFunctions,
            List<ModelDef> models) {
        if (transforms.isEmpty()) {
            return input;
        }

        PreTransformOperator.Builder preTransformFunctionBuilder =
                PreTransformOperator.newBuilder();
        for (TransformDef transform : transforms) {
            if (transform.isValidProjection()) {
                preTransformFunctionBuilder.addTransform(
                        transform.getSourceTable(),
                        transform.getProjection().orElse(null),
                        transform.getFilter().orElse(null),
                        transform.getPrimaryKeys(),
                        transform.getPartitionKeys(),
                        transform.getTableOptions());
            }
        }

        List<UdfDef> allFunctions = new ArrayList<>(udfFunctions);
        allFunctions.addAll(convertModelsToUdfs(models));

        preTransformFunctionBuilder.addUdfFunctions(
                allFunctions.stream().map(this::udfDefToTuple2).collect(Collectors.toList()));
        return input.transform(
                "Transform:Schema", new EventTypeInfo(), preTransformFunctionBuilder.build());
    }

    public DataStream<Event> translatePostTransform(
            DataStream<Event> input,
            List<TransformDef> transforms,
            String timezone,
            List<UdfDef> udfFunctions,
            List<ModelDef> models) {
        if (transforms.isEmpty()) {
            return input;
        }

        PostTransformOperator.Builder postTransformFunctionBuilder =
                PostTransformOperator.newBuilder();
        for (TransformDef transform : transforms) {
            if (transform.isValidProjection() || transform.isValidFilter()) {
                postTransformFunctionBuilder.addTransform(
                        transform.getSourceTable(),
                        transform.isValidProjection() ? transform.getProjection().get() : null,
                        transform.isValidFilter() ? transform.getFilter().get() : null,
                        transform.getPrimaryKeys(),
                        transform.getPartitionKeys(),
                        transform.getTableOptions());
            }
        }
        postTransformFunctionBuilder.addTimezone(timezone);

        List<UdfDef> allFunctions = new ArrayList<>(udfFunctions);
        allFunctions.addAll(convertModelsToUdfs(models));

        postTransformFunctionBuilder.addUdfFunctions(
                allFunctions.stream().map(this::udfDefToTuple2).collect(Collectors.toList()));
        return input.transform(
                "Transform:Data", new EventTypeInfo(), postTransformFunctionBuilder.build());
    }

    private List<UdfDef> convertModelsToUdfs(List<ModelDef> models) {
        return models.stream().map(this::modelToUdf).collect(Collectors.toList());
    }

    private UdfDef modelToUdf(ModelDef model) {
        String udfName = model.getName();
        String serializedParams = serializeModelParams(model);
        return new UdfDef(udfName, MODEL_UDF_CLASSPATH, serializedParams);
    }

    private String serializeModelParams(ModelDef model) {
        return String.format(
                "{\"name\":\"%s\",\"host\":\"%s\",\"apiKey\":\"%s\"}",
                model.getName(), model.getHost(), model.getApiKey());
    }

    private Tuple2<String, String> udfDefToTuple2(UdfDef udf) {
        if (MODEL_UDF_CLASSPATH.equals(udf.getClasspath())) {
            // For ModelUdf, encode the serialized params into the name
            return Tuple2.of(
                    encodeNameWithParams(udf.getName(), udf.getSerializedParams()),
                    udf.getClasspath());
        } else {
            // For regular UDFs, just use the name and classpath
            return Tuple2.of(udf.getName(), udf.getClasspath());
        }
    }

    private String encodeNameWithParams(String name, String params) {
        return params != null ? name + PARAM_SEPARATOR + params : name;
    }
}
