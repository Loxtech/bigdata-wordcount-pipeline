echo "[STEP 1] Fetching and preparing data..."
python3 /home/loke/BigData/codes/batch/Extract.py

echo "[STEP 2] Cleaning up previous output if it exists..."
hdfs dfs -rm -r /user/loke/output_dir

echo "[STEP 3] Running MapReduce job..."
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.4.2.jar \
  -files /home/loke/BigData/codes/batch/mapper.py,/home/loke/BigData/codes/batch/reduce.py \
  -input /user/loke/input_dir/AChristmasCarol_CharlesDickens_English.txt \
  -output /user/loke/output_dir \
  -mapper "python3 mapper.py" \
  -reducer "python3 reduce.py"

# Check if the MapReduce job was successful before loading
if [ $? -eq 0 ]; then
  echo "[STEP 4] Loading and displaying results..."
  hdfs dfs -cat /user/loke/output_dir/part-00000
else
  echo "[STEP 3 FAILED] MapReduce job failed. Skipping load."
fi