import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MapReducePooled {

    public static void main(String[] args) {

        // Read in some data
        final HashMap<String, String> input;
        try {
            input = DataReader.readData();
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        MapReducePooled.timeStart();

        // APPROACH #3: Distributed MapReduce
        final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

        // Create our thread pool
        ExecutorService executor = Executors.newFixedThreadPool(5);

        // MAP:

        final List<MappedItem> mappedItems = new LinkedList<MappedItem>();

        final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
            @Override
            public synchronized void mapDone(String file, List<MappedItem> results) {
                mappedItems.addAll(results);
            }
        };

        Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
        while(inputIter.hasNext()) {
            Map.Entry<String, String> entry = inputIter.next();
            final String file = entry.getKey();
            final String contents = entry.getValue();

            executor.execute(() -> map(file, contents, mapCallback));
        }

        // wait for mapping phase to be over:
        executor.shutdown();

        while(!executor.isTerminated());

        // GROUP:

        Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

        Iterator<MappedItem> mappedIter = mappedItems.iterator();
        while(mappedIter.hasNext()) {
            MappedItem item = mappedIter.next();
            String word = item.getWord();
            String file = item.getFile();
            List<String> list = groupedItems.get(word);
            if (list == null) {
                list = new LinkedList<String>();
                groupedItems.put(word, list);
            }
            list.add(file);
        }

        // REDUCE:

        final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
            @Override
            public synchronized void reduceDone(String k, Map<String, Integer> v) {
                output.put(k, v);
            }
        };

        executor = Executors.newFixedThreadPool(5);

        Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
        while(groupedIter.hasNext()) {
            Map.Entry<String, List<String>> entry = groupedIter.next();
            final String word = entry.getKey();
            final List<String> list = entry.getValue();

            executor.execute(() -> reduce(word, list, reduceCallback));
        }

        // wait for mapping phase to be over:
        executor.shutdown();
        while(!executor.isTerminated());

        MapReducePooled.timeStop();
        System.out.println(String.format("Execution time: %dns", MapReducePooled.duration));
        System.out.println(output);
    }

    public interface MapCallback<E, V> {
        void mapDone(E key, List<V> values);
    }

    public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
        String[] words = contents.trim().split("\\s+");
        List<MappedItem> results = new ArrayList<>(words.length);
        for(String word: words) {
            results.add(new MappedItem(word, file));
        }
        callback.mapDone(file, results);
    }

    public interface ReduceCallback<E, K, V> {
        void reduceDone(E e, Map<K, V> results);
    }

    public static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {

        Map<String, Integer> reducedList = new HashMap<>();
        for(String file: list) {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences + 1);
            }
        }
        callback.reduceDone(word, reducedList);
    }

    private static class MappedItem {

        private final String word;
        private final String file;

        public MappedItem(String word, String file) {
            this.word = word;
            this.file = file;
        }

        public String getWord() {
            return word;
        }
        public String getFile() {
            return file;
        }

        @Override
        public String toString() {
            return "[\"" + word + "\",\"" + file + "\"]";
        }
    }

    static long startTime, stopTime, duration;
    public static void timeStart() {
        startTime = System.nanoTime();
    }

    public static void timeStop() {
        stopTime = System.nanoTime();
        duration = stopTime - startTime;
    }
} 

