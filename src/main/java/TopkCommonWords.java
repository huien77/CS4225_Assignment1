// Matric Number: A0221841N
// Name: Tan Hui En
// TopkCommonWords.java

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

public class TopkCommonWords {
    // Mapper for file 1
    public static class TokenizerMapper1 extends Mapper<Object, Text, Text, IntWritable> {
        private ArrayList<String> stopWords = null;
        private TreeMap<String, Integer> tmap;

        /*
        setup() is called once before map(), it is used to load the stop words
        from the text file into an array list, stopWords
        */
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            stopWords = new ArrayList<>();

            URI[] cacheFiles = context.getCacheFiles();

            try {
                String line = "";

                // FileSystem is an abstract base class for generic filesystem
                // All code that uses HDFS should be written to FileSystem object
                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path getFilePath = new Path(cacheFiles[0].toString());

                /*
                Open the file by using InputStreamReader to convert input byte stream
                to character stream and wrap it in BufferedReader
                 */
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(fs.open(getFilePath)));

                // To store stop words in the file to stopWords array list
                while ((line = reader.readLine()) != null) {
                    stopWords.add(line);
                }
            } catch (Exception e) {
                System.out.println("Unable to read file");
            }
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] words = value.toString().split("[ \t\n\r\f]");
            tmap = new TreeMap<>();

            for (String tempWord : words) {
                if (!tempWord.isEmpty() && !stopWords.contains(tempWord)) {
                    if (tmap.containsKey(tempWord)) {
                        tmap.put(tempWord, tmap.get(tempWord) + 1);
                    } else {
                        tmap.put(tempWord, 1);
                    }
                }
            }
        }

        /*
        cleanup() is called once at the end in the lifetime of Mapper
        use context.write() in cleanup() if the whole block of data needs to be
        processed first before output
        */
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, Integer> e : tmap.entrySet()) {
                // add '1' to the end of the word to indicate the word is from file1
                // so that is does not merge with file2 in the reducer
                String key = e.getKey() + "1";
                Integer value = e.getValue();

                context.write(new Text(key), new IntWritable(value));
            }
        }
    }

    // Mapper for file 2
    public static class TokenizerMapper2 extends Mapper<Object, Text, Text, IntWritable> {
        private ArrayList<String> stopWords = null;
        private TreeMap<String, Integer> tmap;

        /*
        setup() is called once before map(), it is used to load the stop words
        from the text file into an array list, stopWords
        */
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            stopWords = new ArrayList<>();

            URI[] cacheFiles = context.getCacheFiles();

            try {
                String line = "";

                // FileSystem is an abstract base class for generic filesystem
                // All code that uses HDFS should be written to FileSystem object
                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path getFilePath = new Path(cacheFiles[0].toString());

                /*
                Open the file by using InputStreamReader to convert input byte stream
                to character stream and wrap it in BufferedReader
                 */
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(fs.open(getFilePath)));

                // To store stop words in the file to stopWords array list
                while ((line = reader.readLine()) != null) {
                    stopWords.add(line);
                }
            } catch (Exception e) {
                System.out.println("Unable to read file");
            }
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] words = value.toString().split("[ \t\n\r\f]");
            tmap = new TreeMap<>();

            for (String tempWord : words) {
                if (!tempWord.isEmpty() && !stopWords.contains(tempWord)) {
                    if (tmap.containsKey(tempWord)) {
                        tmap.put(tempWord, tmap.get(tempWord) + 1);
                    } else {
                        tmap.put(tempWord, 1);
                    }
                }
            }
        }

        /*
        cleanup() is called once at the end in the lifetime of Mapper
        use context.write() in cleanup() if the whole block of data needs to be
        processed first before output
        */
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, Integer> e : tmap.entrySet()) {
                // add '2' to the end of the word to indicate the word is from file2
                // so that is does not merge with file1 in the reducer
                String key = e.getKey() + "2";
                Integer value = e.getValue();

                context.write(new Text(key), new IntWritable(value));
            }
        }
    }

    // Reducer implementation
    public static class IntSumReducer extends Reducer<Text,IntWritable,IntWritable,Text> {
        // stores the bigger frequency of each word
        private TreeMap<String, Integer> result;
        // checks if the two files have common words
        private TreeMap<String, Integer> common;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            result = new TreeMap<>();
            common = new TreeMap<>();
        }

        // Sorting TreeMap by value
        public static <K, V extends Comparable<V>> Map<K, V> valueSort(final Map<K, V> map) {
            // return comparison results of values of two keys
            Comparator<K> valueComparator = (k1, k2) -> {
                int comp = map.get(k1).compareTo(map.get(k2));
                if (comp == 0)
                    // sort descending
                    return -1;
                else
                    return comp;
            };

            // SortedMap created using the comparator
            Map<K, V> sorted = new TreeMap<>(valueComparator);

            sorted.putAll(map);

            return sorted;
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            int keyLength = key.toString().length();
            String word = key.toString().substring(0, keyLength);

            for (IntWritable val : values) {
                sum += val.get();
            }

            if (result.containsKey(word)) {
                int currentCount = result.get(word);

                if (currentCount < sum) {
                    result.put(word, sum);
                }
            } else {
                result.put(word, sum);
            }

            if (common.containsKey(word)) {
                // int currentCount = common.get(word);
                common.put(word, 2);
            } else {
                common.put(word, 1);
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            Map<String, Integer> sortedMap = valueSort(result);

            for (Map.Entry<String, Integer> e : sortedMap.entrySet()) {
                Integer frequency = e.getValue();
                String word = e.getKey();

                if (common.get(word) == 2) {
                    context.write(new IntWritable(frequency), new Text(word));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // Reads the default configuration
        Configuration conf = new Configuration();

        // Initialise job
        Job job = Job.getInstance(conf, "topkcommonwords");

        // Assign driver class name
        job.setJarByClass(TopkCommonWords.class);

        // Set mapper classes
        job.setMapperClass(TokenizerMapper1.class);
        job.setMapperClass(TokenizerMapper2.class);

        // Set reducer class
        job.setReducerClass(IntSumReducer.class);

        // Set key and value type output for map
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Set key and value type output for reduce
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // Add cache file, it is used for read-only files, in this case, stopwords.txt
        try {
            job.addCacheFile(new URI(args[2]));
        } catch (Exception e) {
            System.out.println("File not added");
        }

        // Multiple input files
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,
                TokenizerMapper1.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class,
                TokenizerMapper2.class);

        // Configure output path
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        // Exit job when flag is false
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
