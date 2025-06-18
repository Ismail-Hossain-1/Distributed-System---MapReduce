Writing a MapReduce program **from scratch** (using Hadoop) involves understanding a few key concepts, classes, and structure. It’s not like regular Java development—you’re building **distributed data processing logic** that runs on multiple machines.

---

### ✅ What You Need to Know

To write a simple Hadoop MapReduce program from scratch, you should understand these essentials:

---

### 1. **MapReduce Programming Model (Concept)**

* **Mapper**: Processes input data line by line and outputs key-value pairs.
* **Reducer**: Aggregates all values by key and outputs final key-value pairs.
* **Combiner** *(optional)*: A mini-reducer run on mapper output before it's sent across the network.
* **Driver**: Java class that sets up and runs the job.

Think of it like:

```plaintext
Input Text File → [Mapper → Shuffle & Sort → Reducer] → Output File
```

---

### 2. **Required Java Knowledge**

You should know:

* Classes and objects
* Inheritance and method overriding
* File I/O
* Java generics (for Hadoop types like `Text`, `IntWritable`)
* Basic understanding of how to compile and run Java with dependencies

---

### 3. **Key Hadoop Classes**

| Role        | Class Name                            |
| ----------- | ------------------------------------- |
| Mapper      | `org.apache.hadoop.mapreduce.Mapper`  |
| Reducer     | `org.apache.hadoop.mapreduce.Reducer` |
| Writable    | `IntWritable`, `Text`, etc.           |
| Job Control | `org.apache.hadoop.mapreduce.Job`     |

---

### 🔧 4. **Minimal Setup Example: Word Count**

Here’s the simplest version of a MapReduce Word Count job:

```java
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");

        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class); // optional, improves performance
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));   // input file path
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // output directory

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

---

### 🧱 5. **Build & Run the Program**

#### 📦 Compile into a JAR

You’ll need to compile with Hadoop dependencies. If you're not using Maven, something like:

```bash
javac -classpath $(hadoop classpath) -d . WordCount.java
jar cf wordcount.jar WordCount*.class
```

#### ▶️ Run with Hadoop

```bash
hadoop jar wordcount.jar WordCount /input/path /output/path
```

---

### 🔍 Summary: Steps to Build a Simple Program

| Step | What to Do                                |
| ---- | ----------------------------------------- |
| 1    | Write `Mapper` class                      |
| 2    | Write `Reducer` class                     |
| 3    | Write `main()` method to set up the job   |
| 4    | Compile it with Hadoop dependencies       |
| 5    | Package into a JAR file                   |
| 6    | Run it using `hadoop jar` on your cluster |

---

### 🧠 Next Level: If You Want to Learn More

* Learn about **combiners**, **partitioners**, and **custom comparators**
* Use **multiple reducers**
* Handle **large datasets and failures**
* Look into **Apache Spark**, which is easier and more efficient than raw MapReduce

---

Let me know if you want a ready-to-run example you can test locally (with input/output files), or if you're using a cluster like AWS EMR or a local Hadoop setup.






Great question! The generic types in `extends Mapper<...>` and `extends Reducer<...>` **are NOT fixed** — they are fully customizable based on your data.

---

## 📌 Quick Rule:

```java
Mapper<INPUT_KEY, INPUT_VALUE, OUTPUT_KEY, OUTPUT_VALUE>
Reducer<INPUT_KEY, INPUT_VALUE, OUTPUT_KEY, OUTPUT_VALUE>
```

You define these types based on:

* What kind of data is being **read**
* What kind of key/value you want to **emit**

---

## 🔍 Example: Word Count

```java
Mapper<Object, Text, Text, IntWritable>
```

### Why?

| Type          | Meaning                                  |
| ------------- | ---------------------------------------- |
| `Object`      | Input key (offset of the line, not used) |
| `Text`        | Input value (entire line of text)        |
| `Text`        | Output key (each word)                   |
| `IntWritable` | Output value (count = 1)                 |

### Reducer:

```java
Reducer<Text, IntWritable, Text, IntWritable>
```

| Type          | Meaning                    |
| ------------- | -------------------------- |
| `Text`        | Input key (a word)         |
| `IntWritable` | Input values (list of 1s)  |
| `Text`        | Output key (word)          |
| `IntWritable` | Output value (total count) |

---

## 🧠 So, How to Decide?

Ask yourself these questions:

### **1. What is your input format?**

Default: Line-by-line from text file

* Then: `Mapper<Object, Text, ..., ...>`

### **2. What do you want your mapper to emit?**

Example:

* Counting visits per IP → `Mapper<Object, Text, Text (IP), IntWritable (1)>`
* Sales sum → `Mapper<Object, Text, Text (ProductID), FloatWritable (Price)>`

### **3. What keys and values does your reducer expect to process?**

* It receives **all values grouped by key**
* Output can be anything:

  * `Text`, `IntWritable`, `FloatWritable`, `NullWritable`, etc.

---

## 🧪 Example 2: Summing Sales by Product

Assume CSV: `productId,price`

```java
Mapper<Object, Text, Text, FloatWritable>
Reducer<Text, FloatWritable, Text, FloatWritable>
```

* Mapper emits: `<productId, price>`
* Reducer receives: `<productId, list of prices>` → sums → emits total sales

---

## ✅ Writable Types You Can Use

| Java Type | Hadoop Type      |
| --------- | ---------------- |
| `int`     | `IntWritable`    |
| `long`    | `LongWritable`   |
| `float`   | `FloatWritable`  |
| `double`  | `DoubleWritable` |
| `String`  | `Text`           |
| null      | `NullWritable`   |

---

## 💡 Final Tip

When unsure, print debug output inside `map()` and `reduce()` to test what keys and values are flowing.

Would you like a small practice challenge to try customizing this?

|
|
|

— the method signatures for `map()` and `reduce()` **must** have **exactly three parameters**:

---

## 🔹 `map()` Signature (always):

```java
map(KEYIN key, VALUEIN value, Context context)
```

* `KEYIN`: Input key (e.g., line offset)
* `VALUEIN`: Input value (e.g., line of text)
* `Context`: Used to emit output (`context.write(...)`)

🟡 You **must not change the number of parameters**. The `Context` is required for Hadoop to interact with the mapper's output.

---

## 🔹 `reduce()` Signature (always):

```java
reduce(KEYIN key, Iterable<VALUEIN> values, Context context)
```

* `KEYIN`: The grouped key (e.g., a word)
* `Iterable<VALUEIN>`: All values for that key (e.g., \[1, 1, 1])
* `Context`: Used to emit reduced output

Again, **fixed three parameters** — this is what Hadoop internally expects.

---

## ⛔️ You Cannot Do:

* `map(Text a, Text b)` ← ❌ won't compile or run
* `reduce(String key, List<IntWritable> vals)` ← ❌ invalid types + missing context

---

### ✅ Correct Customization Is:

You **choose** the generic types in:

```java
Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
```

…but the `map()` and `reduce()` method **must** follow the three-parameter structure.

---

Let me know if you want a template to follow for any custom logic (e.g., sales, weather data, etc.).


|---|
| |
|---|

Excellent — now we're digging into the **heart of the Reducer logic**, where Hadoop actually processes those **"arrays"** (really, `Iterable<IntWritable>`) that it grouped from the Mapper's output.

Let’s walk through it clearly.

---

## ✅ Here's the Key Reducer Code Again:

```java
public static class AvgWordLengthReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private int totalLength = 0;
    private int totalCount = 0;

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();  // <- This is where the addition happens
        }

        if (key.toString().equals("length")) {
            totalLength = sum;
        } else if (key.toString().equals("count")) {
            totalCount = sum;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (totalCount > 0) {
            int avg = totalLength / totalCount;
            context.write(new Text("Average Word Length"), new IntWritable(avg));
        }
    }
}
```

---

## ✅ Now Let's Break It Down

### 🔹 1. **This method is called once for each key (e.g., "length", "count"):**

```java
public void reduce(Text key, Iterable<IntWritable> values, Context context)
```

* Hadoop guarantees that **all values with the same key are grouped and passed together**.
* You can think of `values` as an array:

  * If `key == "length"` → `values = [4, 5, 6, 7, ...]`
  * If `key == "count"`  → `values = [1, 1, 1, 1, ...]`

---

### 🔹 2. **This loop handles the actual addition:**

```java
int sum = 0;
for (IntWritable val : values) {
    sum += val.get();
}
```

* This sums up the elements of the "array"
* Once the sum is computed, we store it in a class variable

---

### 🔹 3. **This part checks which key we are processing:**

```java
if (key.toString().equals("length")) {
    totalLength = sum;
} else if (key.toString().equals("count")) {
    totalCount = sum;
}
```

* So if the Reducer is handling `"length"` key group, it sets `totalLength`
* If it's the `"count"` key group, it sets `totalCount`

---

### 🔹 4. **Then in `cleanup()` we calculate and output the result:**

```java
int avg = totalLength / totalCount;
context.write(new Text("Average Word Length"), new IntWritable(avg));
```

* This happens **after** all `reduce()` calls are complete
* It's used when you want to **output something after aggregating multiple keys**

---

## ✅ Summary Table

| Role                        | Code                             | Description                              |
| --------------------------- | -------------------------------- | ---------------------------------------- |
| Handle grouped values       | `for (IntWritable val : values)` | Iterate like an array                    |
| Sum values                  | `sum += val.get();`              | Adds up word lengths or counts           |
| Track totals by key type    | `if (key.equals(...))`           | Distinguish between "length" and "count" |
| Output after all reductions | `cleanup()` method               | Outputs final average                    |

---

## 🧠 Visualization

Mapper output:

```
("length", 4)
("count", 1)
("length", 5)
("count", 1)
("length", 6)
("count", 1)
```

Reducer receives:

```
"length" → [4, 5, 6]   → sum = 15
"count"  → [1, 1, 1]   → sum = 3
```

Then:

```
avg = 15 / 3 = 5
```

Output:

```
("Average Word Length", 5)
```

---


---

 **the Reducer's `reduce()` method runs once per key**, **not once per all keys combined.**

---

### How it actually works:

* Hadoop groups the mapper outputs by key.
* For each unique key, Hadoop calls **`reduce(key, Iterable<values>, context)` once**.
* So, **the for-loop inside `reduce()` processes *only the values for that one key*.**

---

### So:

* First, Hadoop calls:

```java
reduce("count", Iterable of all count values, context)
```

The for-loop sums **all the counts** (like 1 + 1 + 1 + ...), then you assign:

```java
totalCount = sum;
```

* Then, Hadoop calls:

```java
reduce("length", Iterable of all length values, context)
```

The for-loop sums **all the lengths** (like 4 + 5 + 6 + ...), then you assign:

```java
totalLength = sum;
```

---

### To summarize:

| Call # | Key      | Values             | What happens inside `reduce()`        |
| ------ | -------- | ------------------ | ------------------------------------- |
| 1      | "count"  | \[1, 1, 1, 1, ...] | Sum all counts and set `totalCount`   |
| 2      | "length" | \[4, 5, 6, 7, ...] | Sum all lengths and set `totalLength` |

---

### So the for-loop **only sums the values for one key at a time**, not both keys in the same call.

---

### That's why:

```java
if (key.toString().equals("length")) {
    totalLength = sum;
} else if (key.toString().equals("count")) {
    totalCount = sum;
}
```

happens **after the summation for that key only.**

---

### Then after both calls have finished (i.e., after all keys processed),

the `cleanup()` method runs — which uses the stored `totalLength` and `totalCount` to calculate the average and write the final output.

---

### So the lifecycle is like this:

1. `reduce("count", [...])` → sums counts → sets `totalCount`
2. `reduce("length", [...])` → sums lengths → sets `totalLength`
3. `cleanup()` → calculates and outputs average

---



