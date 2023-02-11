package util;

import org.apache.spark.sql.Dataset;

import java.util.Collection;
import java.util.Map;

public final class ParameterValidator {
    private ParameterValidator() throws InstantiationException {
        throw new InstantiationException("Can not instantiate Utility class");
    }

    public static <E> boolean isEmptyCollection(Collection<E> collection) {
        return collection == null || collection.isEmpty();
    }

    public static <K, V> boolean isEmptyMap(Map<K, V> map) {
        return map == null || map.isEmpty();
    }

    public static <E> boolean isEmptyDataset(Dataset<E> dataset) {
        return dataset == null || dataset.isEmpty();
    }
}
