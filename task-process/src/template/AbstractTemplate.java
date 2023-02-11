package template;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public abstract class AbstractTemplate {

    public SparkSession init(String master, String appName) {
        return SparkSession.builder().master(master).appName(appName).getOrCreate();
    }

    public void steps(String master, String appName, String inputPath, String outputPath) {
        SparkSession session = init(master, appName);
        Dataset<Row> parse = parseDataset(session, inputPath);
        Dataset<Row> extract = extractedFields(parse);
        Dataset<Row> sortedScore = getSortedSetByScore(extract);
        Double median = calculateMedian(sortedScore);
        List<Double> topThree = topThreeScores(sortedScore);
        Dataset<Row> getTargetAndDisease = getTargetAndDisease(extract);
        Dataset<Row> sortedTableAscending = sortedByMedian(getTargetAndDisease, median, topThree);
        outputTableJsonFormat(sortedTableAscending, outputPath);
        System.out.println("Count how many target-target pairs share a connection with diseases = " + countShareConnection(getTargetAndDisease));
    }

    public abstract Dataset<Row> parseDataset(SparkSession session, String path);

    public abstract Dataset<Row> extractedFields(Dataset<Row> dataset);

    public abstract Double calculateMedian(Dataset<Row> dataset);

    public abstract List<Double> topThreeScores(Dataset<Row> dataset);

    public abstract Dataset<Row> sortedByMedian(Dataset<Row> dataset, Double median, List<Double> topThree);

    public abstract void outputTableJsonFormat(Dataset<Row> dataset, String path);

    public abstract int countShareConnection(Dataset<Row> dataset);

    public abstract Dataset<Row> getTargetAndDisease(Dataset<Row> dataset);

    public abstract Dataset<Row> getSortedSetByScore(Dataset<Row> dataset);
}
