This code is written in a java programming language, but I don't mind learning any programming language :).

# Challenges that I faced when coding:
- I did not have apache spark jars to work with it in java, so I downloaded them through https://spark.apache.org/downloads.html
- I did not have hadoop.dll and winutils.exe on windows. To solve this issue, i followed this link to solve this issue https://cwiki.apache.org/confluence/display/HADOOP2/WindowsProblems

# Assumptions:
- for point 3 is mentioned in the task: I assumed that we joined the two datasets targets & diseases because I did now know where to get `target.id` &  `disease.id` fields in the datasets
- for point 4 which is mentioned in the task: I did not know where to get and find the `target.approvedSymbol` and `disease.name` fields to add them to an output dataset.
- for point `Count how many target-target pair share connection with at least two diseases`: I was thinking in two different solutions, one is building directed graph of connections give random numbers for each `target` and `disease` and building connectivity for each target to disease and count each target connected to X disease (for example). Another approach is using Map which key is `disease` string and value is `target` string, and if current key (disease) is exist in map, append target value to it, because we need to record each `disease` how many `targets` refer to this key (`disease`) 

# Inputs & Outputs:
- you can find input dataset from path 'dataset\eva-dataset.json' and output from 'dataset\output\part-00000-3f7b4209-9829-46a1-8ced-ab7aeeedc344-c000.json'
