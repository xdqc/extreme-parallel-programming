package nz.ac.waikato.phototool;

import java.awt.Color;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;


/**
 * A simple command line tool for manipulating photos in bulk.
 * <p>
 * (This is also a demo of how to write very inefficient Java!)
 *
 * @author Mark.Utting
 */
public class PhotoTool {

    /**
     * The name or filename of the original photo.
     */
    private final String photoName;

    /**
     * A stack of edited photos.  Original photo is entry 0.
     */
    private final List<Picture> pictures = new ArrayList<>();

    /**
     * Construct a photo editor for the given photo.
     */
    public PhotoTool(String filename) {
        photoName = filename;
        pictures.add(new Picture(filename));
    }

    /**
     * For internal use.
     *
     * @return the top photo on the stack.
     */
    protected Picture getCurrentPhoto() {
        return pictures.get(pictures.size() - 1);
    }

    /**
     * @return the number of photos currently on the stack.  At least one.
     */
    public int getStackSize() {
        return pictures.size();
    }

    /**
     * @return the width in pixels of the current photo.
     */
    public int getWidth() {
        return getCurrentPhoto().width();
    }

    /**
     * @return the width in pixels of the current photo.
     */
    public int getHeight() {
        return getCurrentPhoto().height();
    }

    /**
     * Pop the current photo off the stack, returning to the previous one.
     * The original photo is never popped off, so the stack will never become empty.
     */
    public void undo() {
        if (pictures.size() > 1) {
            pictures.remove(pictures.size() - 1);
        }
    }

    /**
     * Show the current top photo in a popup window.
     * Does not block the program.
     */
    public void show() {
        getCurrentPhoto().show(photoName);
    }

    /**
     * Create a grayscale version of the current photo and push it on the stack.
     */
    public void grayscale() {
        Picture newPic = new Picture(getWidth(), getHeight());
        for (int y = 0; y < getHeight(); y++) {         // Task 2.1 Stop boxing!
            for (int x = 0; x < getWidth(); x++) {      // Task 2.1 Stop boxing!
                Color pixel = getCurrentPhoto().get(x, y);
                int average = (pixel.getRed() + pixel.getGreen() + pixel.getBlue()) / 3;    // Task 2.1 Stop boxing!
                newPic.set(x, y, new Color(average, average, average));
            }
        }
        pictures.add(newPic);
    }

    /**
     * Create a sepia version of the current photo and push it on the stack.
     * See <a href="http://www.techrepublic.com/blog/howdoi/how-do-i-convert-images-to-grayscale-and-sepia-tone-using-c/120">http://www.techrepublic.com/blog/howdoi/how-do-i-convert-images-to-grayscale-and-sepia-tone-using-c/120</a>.
     */
    public void sepia() {
        Picture newPic = new Picture(getWidth(), getHeight());
        for (int y = 0; y < getHeight(); y++) { // Task 2.1 Stop boxing!
            for (int x = 0; x < getWidth(); x++) { // Task 2.1 Stop boxing!
                Color pixel = getCurrentPhoto().get(x, y);
                int red = clamp((pixel.getRed() * .393) + (pixel.getGreen() * .769) + (pixel.getBlue() * .189));
                int green = clamp((pixel.getRed() * .349) + (pixel.getGreen() * .686) + (pixel.getBlue() * .168));
                int blue = clamp((pixel.getRed() * .272) + (pixel.getGreen() * .534) + (pixel.getBlue() * .131));
                newPic.set(x, y, new Color(red, green, blue));
            }
        }
        pictures.add(newPic);
    }

    /**
     * Scale the current photo to half its size and push it on the stack.
     */
    public void half() {
        int scaleBy = 2;
        Picture newPic = new Picture(getWidth() / scaleBy, getHeight() / scaleBy);
        for (int y = 0; y < getHeight() / scaleBy; y++) { // Task 2.1 Stop boxing!
            for (int x = 0; x < getWidth() / scaleBy; x++) { // Task 2.1 Stop boxing!
                Color pixel = getCurrentPhoto().get(x * scaleBy, y * scaleBy);
                newPic.set(x, y, pixel);
            }
        }
        pictures.add(newPic);
    }

    /**
     * Save the current photo with the same name, suffixed with "_edited".
     */
    public void save() {
        final String newName;
        if (photoName.endsWith(".png")) {
            newName = photoName.substring(0, photoName.length() - 4) + "_edited.png";
        } else if (photoName.endsWith(".jpg")) {
            newName = photoName.substring(0, photoName.length() - 4) + "_edited.jpg";
        } else {
            System.err.println("WARNING: could not save " + photoName + ".  Must be .png/.jpg");
            return;
        }
        try {
            getCurrentPhoto().save(newName);
        } catch (IOException ex) {
            System.err.println("WARNING: IO error while saving " + photoName + ": " + ex.getMessage());
        }
    }

    /**
     * @return value restricted to the range 0..255.
     */
    private int clamp(double value) {
        if (value <= 0) {
            return 0;
        } else if (value >= 255) {
            return 255;
        } else {
            return (int) value;
        }
    }

    /**
     * Find a photo-editing method by name.
     *
     * @param arg the name of a public zero-parameter method in this class.
     * @return the method if it exists, else null.
     */
    private static Method findCmd(String arg) {
        try {
            return PhotoTool.class.getMethod(arg, new Class<?>[0]);
        } catch (NoSuchMethodException | SecurityException e) {
            return null;
        }
    }

    /**
     * Displays a help message and exits with an error code.
     */
    public static void help() {
        System.out.println("Arguments: cmd1 cmd2 cmd3...  photo1 photo2 photo3...");
        System.exit(1);
    }

    /**
     * @param args cmds... photos...  Where the available cmds are all the public
     *             no-argument methods in this class (grayscale, sepia, half, undo, show, save).
     */
    public static void main(String[] args) {
        if (args.length < 2) {
            help();
        }
        List<Method> cmds = new ArrayList<>();
        /**
         * Task 1:
         * Use List<String> to store photo names to be processed, rather than using List<PhotoTool> to store
         * large PhotoTool objects
         */
        List<String> photos = new ArrayList<>();
        int argNum = 0;
        // record the command sequence
        for (; argNum < args.length; argNum++) {
            String arg = args[argNum];
            Method method = findCmd(arg);
            if (method == null) {
                break; // start processing file names
            }
            cmds.add(method);
        }
        // record the photos
        for (; argNum < args.length; argNum++) {
            String arg = args[argNum];
            if (new File(arg).canRead()) {
                photos.add(arg);
            } else {
                System.err.println("ERROR: unknown command or photo: " + arg);
                System.exit(2);
            }
        }
        if (cmds.isEmpty() || photos.isEmpty()) {
            help();
        }
        for (String photo : photos) {
            PhotoTool tool = new PhotoTool(photo);
            //Please do not remove or change the format of this output message
            System.out.println("processing " + tool.photoName + "...");

            long totalTime = 0;
            int total = 10;
            //Please remember to remove this loop when submitting it
            for (int iter = 0; iter < total; iter++) {
                while (tool.getStackSize() > 1) {
                    tool.undo();
                }
                //do not move or change this time measurement statement
                final long time0 = System.nanoTime();

                // Here goes the code you want to measure the speed of...
                for (Method cmd : cmds) {
                    try {
                        cmd.invoke(tool);
                    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
                        System.err.println("ERROR executing command " + cmd.getName() + ": " + ex);
                        System.exit(3);
                    }
                }

                //please do not move or change this time measurement statement
                long time1 = System.nanoTime();
                //Please do not remove or change the format of this output message
                System.out.println("Processed " + cmds.size() + " cmds in " + (time1 - time0) / 1E9 + " secs.");
                if (iter > 2) {
                    totalTime += (time1 - time0);
                }
            }
            System.out.println("Average processed " + cmds.size() + " cmds in " + (totalTime / (total - 3)) / 1E9 + " secs.");
        }
    }

}
