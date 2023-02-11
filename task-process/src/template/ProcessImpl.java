package template;

import org.apache.spark.sql.*;
import util.ParameterValidator;

import java.util.*;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.column;

public class ProcessImpl extends AbstractTemplate {

    /*This method is parsing json datasets*/
    @Override
    public Dataset<Row> parseDataset(SparkSession session, String path) {
        Dataset<Row> list = session.read().json(path);

        if (ParameterValidator.isEmptyDataset(list))
            throw new IllegalArgumentException("Dataset is empty");

        return list;
    }

    /*This method is extracting fields from given dataset*/
    @Override
    public Dataset<Row> extractedFields(Dataset<Row> dataset) {
        if (ParameterValidator.isEmptyDataset(dataset))
            throw new IllegalArgumentException("Dataset is empty");

        return dataset.select("targetId", "diseaseId", "score");
    }

    /*This method is calculating median from given dataset*/
    @Override
    public Double calculateMedian(Dataset<Row> dataset) {
        if (ParameterValidator.isEmptyDataset(dataset))
            throw new IllegalArgumentException("Dataset is empty");

        Dataset<Row> scoresDF = dataset.select("score");
        Dataset<Row> sortedScoresAsc = scoresDF.orderBy(col("score").asc());
        List<Row> sortedScoreList = sortedScoresAsc.collectAsList();
        double median = 0.0;

        if (!ParameterValidator.isEmptyDataset(dataset) && !ParameterValidator.isEmptyCollection(sortedScoreList)) {
            int sortedScoreLen = sortedScoreList.size();
            if ((sortedScoreLen & 1) == 1) { // check if length is odd or even
                median = sortedScoreList.get((sortedScoreLen + 1) / 2).getDouble(0);
            } else {
                Double firstScore = sortedScoreList.get((sortedScoreLen) / 2).getDouble(0);
                Double secondScore = sortedScoreList.get((sortedScoreLen + 1) / 2).getDouble(0);
                System.out.println("first score = " + firstScore);
                System.out.println("second score = " + secondScore);
                median = (firstScore + secondScore) / 2;
            }
        }


        System.out.println("Median scores are = " + median);
        return median;
    }

    /*This method is getting top 3 scores from given dataset*/
    @Override
    public List<Double> topThreeScores(Dataset<Row> dataset) {
        if (ParameterValidator.isEmptyDataset(dataset))
            throw new IllegalArgumentException("Dataset is empty");

        List<Double> topThree = new ArrayList<>();
        List<Row> sortedScoreList = dataset.collectAsList();

        if (!ParameterValidator.isEmptyDataset(dataset) && !ParameterValidator.isEmptyCollection(sortedScoreList)) {
            int sortedScoreLen = sortedScoreList.size();

            if (sortedScoreLen - 1 >= 0)
                topThree.add(sortedScoreList.get(sortedScoreLen - 1).getDouble(0));

            if (sortedScoreLen - 2 >= 0)
                topThree.add(sortedScoreList.get(sortedScoreLen - 2).getDouble(0));

            if (sortedScoreLen - 3 >= 0)
                topThree.add(sortedScoreList.get(sortedScoreLen - 3).getDouble(0));
        }

        return topThree;
    }

    /*This method is sorting dataset by median from given dataset*/
    @Override
    public Dataset<Row> sortedByMedian(Dataset<Row> dataset, Double median, List<Double> topThree) {
        if (ParameterValidator.isEmptyDataset(dataset))
            throw new IllegalArgumentException("Dataset is empty");

        Dataset<Row> newDs = dataset.withColumn("median", functions.lit(median));
        newDs = newDs.withColumn("top3", functions.array(topThree.stream().map(functions::lit).toArray(Column[]::new)));
        newDs = newDs.withColumn("target.approvedSymbol", functions.lit("approvedSymbol"));
        newDs = newDs.withColumn("disease.name", functions.lit("name"));
        newDs = newDs.orderBy(col("median").asc());
        return newDs;
    }

    /*This method is output json format file from given dataset*/
    @Override
    public void outputTableJsonFormat(Dataset<Row> dataset, String path) {
        if (ParameterValidator.isEmptyDataset(dataset))
            throw new IllegalArgumentException("Dataset is empty");

        dataset.write().json(path);
    }

    /*This method is count how many target-target pairs share a connection to at least more than two diseases.*/
    @Override
    public int countShareConnection(Dataset<Row> dataset) {
        if (ParameterValidator.isEmptyDataset(dataset))
            throw new IllegalArgumentException("Dataset is empty");

        dataset = dataset.groupBy(column("targetId")).df();
        int count = 0;
        if (!ParameterValidator.isEmptyDataset(dataset)) {
            Map<String, Set<String>> shareConnectionMap = new HashMap<>();
            List<Row> listTargetDisease = dataset.collectAsList();
            listTargetDisease.forEach(row -> {
                String disease = row.getString(1);
                String target = row.getString(0);
                Set<String> targetSet = new HashSet<>();

                if (shareConnectionMap.containsKey(disease)) {
                    targetSet = shareConnectionMap.get(disease);
                }

                targetSet.add(target);
                shareConnectionMap.put(disease, targetSet);
            });

            if (!ParameterValidator.isEmptyMap(shareConnectionMap)) {
                for (Map.Entry<String, Set<String>> entry : shareConnectionMap.entrySet()) {
                    count += entry.getValue().size();
                }
            }
        }

        return count;
    }

    /*This method is getting targets and diseases dataset from given dataset*/
    @Override
    public Dataset<Row> getTargetAndDisease(Dataset<Row> dataset) {
        if (ParameterValidator.isEmptyDataset(dataset))
            throw new IllegalArgumentException("Dataset is empty");

        return dataset.select("targetId", "diseaseId");
    }

    /*This method is sorting dataset ascending by score from given dataset*/
    @Override
    public Dataset<Row> getSortedSetByScore(Dataset<Row> dataset) {
        if (ParameterValidator.isEmptyDataset(dataset))
            throw new IllegalArgumentException("Dataset is empty");

        return dataset.select("score").orderBy(col("score").asc());
    }
}
