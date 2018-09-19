

import sun.security.pkcs11.wrapper.Functions;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;


/**
 * A class provides stream assignment implementation template
 */
public class StreamAssignmentTemplate {


    /**
     * @param file: a file used to create the word stream
     * @return a stream of word strings
     * Implementation Notes:
     * This method reads a file and generates a word stream.
     * In this exercise, a word only contains English letters (i.e. a-z and A-Z), and
     * consists of at least one letter. For example, “The” or “tHe” is a word,
     * but “89B” (containing digits 89) or “things,”
     * (containing the punctuation ",") is not.
     */
    public static Stream<String> toWordStream(String file) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            return br.lines()
                    .parallel()
                    .map(l -> l.split("\\s+"))
                    .flatMap(Arrays::stream)
                    .filter(w -> w.matches("[a-zA-Z]+"));

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * @param file: a file used to create a word stream
     * @return the number of words in the file
     * Implementation Notes:
     * This method
     * (1) uses the toWordStream method to create a word stream from the given file
     * (2) counts the number of words in the file
     * (3) measures the time of creating the stream and counting
     */
    public static long wordCount(String file) {
        long t1 = System.currentTimeMillis();
        Stream<String> wordStream = toWordStream(file);
        long t2 = System.currentTimeMillis();
        System.out.println("Create stream time: " + (t2 - t1) + "ms.");
        long numWords = wordStream.count();
        System.out.println("Count word time: " + (System.currentTimeMillis() - t2) + " ms.");
        return numWords;
    }

    /**
     * @param file: a file used to create a word stream
     * @return a list of the unique words, sorted in a reverse alphabetical order.
     * Implementation Notes:
     * This method
     * (1) uses the toWordStream method to create a word stream from the given file
     * (2) generates a list of unique words, sorted in a reverse alphabetical order
     */
    public static List<String> uniqueWordList(String file) {
        return Objects.requireNonNull(toWordStream(file))
                .distinct()
                .sorted(Comparator.reverseOrder())
                .collect(Collectors.toList());
    }

    /**
     * @param file: a file used to create a word stream
     * @return one of the longest words in the file
     * Implementation Notes:
     * This method
     * (1) uses the toWordStream method to create a word stream from the given file
     * (2) uses Stream.reduce to find the longest word
     */
    public static String longestWord(String file) {
        return Objects.requireNonNull(toWordStream(file))
                .reduce((a, b) -> a.length() >= b.length() ? a : b)
                .get();
    }


    /**
     * @param file: a file used to create a word stream
     * @return the number of words consisting of three letters
     * Implementation Notes:
     * This method
     * (1) uses the toWordStream method to create a word stream from the given file
     * (2) uses Stream.reduce (NO other stream operations)
     * to count the number of words containing three letters.
     * i.e. Your code will look like:
     * return toWordStream(file).reduce(...);
     */
    public static long wordsWithThreeLettersCount(String file) {
        return Objects.requireNonNull(toWordStream(file))
                .reduce(0, (acc, s) -> acc += s.length() == 3 ? 1 : 0, (a, b) -> a + b);
    }

    /**
     * @param file: a file used to create a word stream
     * @return the average length of the words (e.g. the average number of letters in a word)
     * Implementation Notes:
     * This method
     * (1) uses the toWordStream method to create a word stream from the given file
     * (2) uses Stream.reduce (NO other stream operations)
     * to calculate the total length and total number of words
     * (3) the average word length can be calculated separately i.e. total_length/total_number_of_words
     */
    public static double avergeWordlength(String file) {
        long totalLength = toWordStream(file)
                .reduce(0, (acc, s) -> acc += s.length(), (a, b) -> a + b);
        return totalLength / (double) (wordCount(file));
    }

    /**
     * @param file: a file used to create a word stream
     * @return a map contains key-value pairs of a word (i.e. key) and its occurrences (i.e. value)
     * Implementation Notes:
     * This method
     * (1) uses the toWordStream method to create a word stream from the given file
     * (2) uses Stream.collect, Collectors.groupingBy, etc., to generate a map
     * containing pairs of word and its occurrences.
     */
    public static Map<String, Integer> toWordCountMap(String file) {
        return Objects.requireNonNull(toWordStream(file))
                .collect(Collectors.groupingBy(Function.identity(), Collectors.collectingAndThen(Collectors.counting(), Long::intValue)));
    }

    /**
     * @param file: a file used to create a word stream
     * @return a map contains key-value pairs of a letter (i.e. key) and a set of words starting with that letter (i.e. value)
     * Implementation Notes:
     * This method
     * (1) uses the toWordStream method to create a word stream from the given file
     * (2) uses Stream.collect, Collectors.groupingBy, etc., to generate a map containing pairs of a letter
     * and a set of words starting with that letter
     */
    public static Map<String, Set<String>> groupWordByFirstLetter(String file) {
        return Objects.requireNonNull(toWordStream(file))
                .collect(Collectors.groupingBy(w -> String.valueOf(w.charAt(0)), toSet()));
    }


    /**
     * @param pf            that takes two parameters (String s1 and String s2) and
     *                      returns the index of the first occurrence of s2 in s1, or -1 if s2 is not a substring of s1
     * @param targetFile:   a file used to create a line stream
     * @param targetString: the string to be searched in the file
     *                      Implementation Notes:
     *                      This method
     *                      (1) uses BufferReader.lines to read in lines of the target file
     *                      (2) uses Stream operation(s) and BiFuction to
     *                      produce a new Stream that contains a stream of Object[]s with two elements;
     *                      Element 0: the index of the first occurrence of the target string in the line
     *                      Element 1: the text of the line containing the target string
     *                      (3) uses Stream operation(s) to sort the stream of Object[]s in a descending order of the index
     *                      (4) uses Stream operation(s) to print out the first 20 indexes and lines in the following format
     *                      567:<the line text>
     *                      345:<the line text>
     *                      234:<the line text>
     *                      ...
     */
    public static void printLinesFound(BiFunction<String, String, Integer> pf, String targetFile, String targetString) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(targetFile));
            Stream<String> lines = br.lines();

            long t1 = System.currentTimeMillis();

            lines
                    .parallel()
                    .map(l -> new Object() {
                        int idx = pf.apply(l, targetString);
                        String line = l;
                    })
                    .filter(obj -> obj.idx >= 0)
                    .sorted(Comparator.comparingInt(obj -> -obj.idx))
                    .limit(20)
                    .collect(Collectors.toList())
                    //Q10. start a new stream here to ensure the sorted order of print, while still using parallel
                    .stream()
                    .forEach(o -> System.out.println(o.idx + ":" + o.line));

            System.out.println("Found time: " + (System.currentTimeMillis() - t1) + " ms.");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        //test your methods here;
        String file = "/home/comp453/stream/wiki_1.xml";

        System.out.println("Word count: " + wordCount(file));

        System.out.println("Unique words: " + uniqueWordList(file).size());

        System.out.println("Longest word: " + longestWord(file));

        System.out.println("Words with 3 letters: " + wordsWithThreeLettersCount(file));

        System.out.println("Average word length: " + avergeWordlength(file));

        System.out.println("'the' count: " + toWordCountMap(file).get("the"));

        System.out.println("Words start with 'a': " + groupWordByFirstLetter(file).get("a").size());

        printLinesFound(String::indexOf, file, "science");
    }


}
