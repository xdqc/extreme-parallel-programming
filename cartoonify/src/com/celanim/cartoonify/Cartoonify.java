package com.celanim.cartoonify;

import com.nativelibs4java.opencl.*;
import org.bridj.Pointer;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import javax.imageio.ImageIO;

/**
 * Processes lots of photos and uses edge detection and colour reduction to make them cartoon-like.
 * <p>
 * Run <code>main</code> to see the usage message.
 * Each input image, eg. xyz.jpg, is processed and then output to a file called xyz_cartoon.jpg.
 * <p>
 * Implementation Note: this class maintains a stack of images, with the original image being
 * at the bottom of the stack (position 0), and the current image being at the top
 * of the stack (this can be accessed as index -1).  Image processing methods
 * should create a new image (1D array of int pixels in row-major order) and push
 * it on top of the stack.  They should not modify images destructively.
 *
 * @author Mark.Utting
 */
public class Cartoonify {

    /**
     * The number of bits used for each colour channel.
     */
    public static final int COLOUR_BITS = 8;

    /**
     * Each colour channel contains a colour value from 0 up to COLOUR_MASK (inclusive).
     */
    public static final int COLOUR_MASK = (1 << COLOUR_BITS) - 1; // eg. 0xFF

    /**
     * An all-black pixel.
     */
    public final int black = createPixel(0, 0, 0);

    /**
     * An all-white pixel.
     */
    public final int white = createPixel(COLOUR_MASK, COLOUR_MASK, COLOUR_MASK);

    // colours are packed into an int as four 8-bit fields: (0, red, green, blue).
    /**
     * The number of the red channel.
     */
    public static final int RED = 2;

    /**
     * The number of the green channel.
     */
    public static final int GREEN = 1;

    /**
     * The number of the blue channel.
     */
    public static final int BLUE = 0;

    /**
     * What level of colour change should be considered an edge.
     */
    private int edgeThreshold = 128;

    /**
     * Number of values in each colour channel (R, G, B) after quantization.
     */
    private int numColours = 3;

    private boolean debug = false;

    private boolean useGPU = false;

    /**
     * The width of all the images.
     */
    private int width;

    /**
     * The height of all the images.
     */
    private int height;

    /**
     * A stack of images, with the current one at position <code>currImage</code>.
     */
    private int[][] pixels;

    /**
     * The position of the current image in the pixels array. -1 means no current image.
     */
    private int currImage;

    /**
     * Choose the platform and the best GPU device
     */
    private CLContext context = JavaCL.createBestContext(CLPlatform.DeviceFeature.GPU);

    /**
     * OpenCL kernel to calculate Gaussian Blur and Sobel Edge Detect
     */
    private static final String source =
            "int wrap(int pos, int size) {\n" +
                    "    return select(0, select(pos, size - 1 ,pos >= size), pos > 0);\n" +
                    "}\n" +
                    "\n" +
                    "int3 pixel (int p) {\n" +
                    "    return (int3)((p>>16) & 0xFF, (p>>8) & 0xFF, p & 0xFF);\n" +
                    "}\n" +
                    "\n" +
                    "__kernel void gaussianBlur\n" +
                    "(__global int *a, __global int *b,int width, int height)\n" +
                    "{\n" +
                    "    int gid = get_global_id(0);\n" +
                    "\n" +
                    "    int yCentre = gid / width;\n" +
                    "    int xCentre = gid % width;\n" +
                    "\n" +
                    "    int y0 = wrap(yCentre - 2, height);\n" +
                    "    int y1 = wrap(yCentre - 1, height);\n" +
                    "    int y2 = wrap(yCentre, height);\n" +
                    "    int y3 = wrap(yCentre + 1, height);\n" +
                    "    int y4 = wrap(yCentre + 2, height);\n" +
                    "    int x0 = wrap(xCentre - 2, width);\n" +
                    "    int x1 = wrap(xCentre - 1, width);\n" +
                    "    int x2 = wrap(xCentre, width);\n" +
                    "    int x3 = wrap(xCentre + 1, width);\n" +
                    "    int x4 = wrap(xCentre + 2, width);\n" +
                    "\n" +
                    "    int3 sum =\n" +
                    "    pixel(a[y0*width + x0]) * 2 + pixel(a[y0*width + x1]) * 4 + pixel(a[y0*width + x2]) * 5 + pixel(a[y0*width + x3]) * 4 + pixel(a[y0*width + x4]) * 2 +\n" +
                    "    pixel(a[y1*width + x0]) * 4 + pixel(a[y1*width + x1]) * 9 + pixel(a[y1*width + x2]) *12 + pixel(a[y1*width + x3]) * 9 + pixel(a[y1*width + x4]) * 4 +\n" +
                    "    pixel(a[y2*width + x0]) * 5 + pixel(a[y2*width + x1]) *12 + pixel(a[y2*width + x2]) *15 + pixel(a[y2*width + x3]) *12 + pixel(a[y2*width + x4]) * 5 +\n" +
                    "    pixel(a[y3*width + x0]) * 4 + pixel(a[y3*width + x1]) * 9 + pixel(a[y3*width + x2]) *12 + pixel(a[y3*width + x3]) * 9 + pixel(a[y3*width + x4]) * 4 +\n" +
                    "    pixel(a[y4*width + x0]) * 2 + pixel(a[y4*width + x1]) * 4 + pixel(a[y4*width + x2]) * 5 + pixel(a[y4*width + x3]) * 4 + pixel(a[y4*width + x4]) * 2;\n" +
                    "\n" +
                    "    sum = sum/159;\n" +
                    "\n" +
                    "    b[gid] = (sum.x<<16) + (sum.y<<8) + sum.z;\n" +
                    "}\n" +
                    "\n" +
                    "__kernel void sobelEdgeDetect\n" +
                    "(__global int *b, __global int *c,int width, int height, int edgeThreshold)\n" +
                    "{\n" +
                    "    int gid = get_global_id(0);\n" +
                    "\n" +
                    "    int yCentre = gid / width;\n" +
                    "    int xCentre = gid % width;\n" +
                    "\n" +
                    "    int y0 = wrap(yCentre - 1, height);\n" +
                    "    int y1 = wrap(yCentre, height);\n" +
                    "    int y2 = wrap(yCentre + 1, height);\n" +
                    "    int x0 = wrap(xCentre - 1, width);\n" +
                    "    int x1 = wrap(xCentre, width);\n" +
                    "    int x2 = wrap(xCentre + 1, width);\n" +
                    "\n" +
                    "    int3 sumV = 0\n" +
                    "    - pixel(b[y0*width + x0]) + pixel(b[y0*width + x2])\n" +
                    "    - pixel(b[y1*width + x0]) * 2 + pixel(b[y1*width + x2]) * 2\n" +
                    "    - pixel(b[y2*width + x0]) + pixel(b[y2*width + x2]);\n" +
                    "\n" +
                    "    int3 sumH = 0\n" +
                    "    + pixel(b[y0*width + x0]) + pixel(b[y0*width + x1]) * 2 + pixel(b[y0*width + x2])\n" +
                    "    - pixel(b[y2*width + x0]) - pixel(b[y2*width + x1]) * 2 - pixel(b[y2*width + x2]);\n" +
                    "\n" +
                    "    int gV = abs(sumV.x) + abs(sumV.y) + abs(sumV.z);\n" +
                    "    int gH = abs(sumH.x) + abs(sumH.y) + abs(sumH.z);\n" +
                    "\n" +
                    "    c[gid] = select(0xFFFFFF, 0,(gV+gH)>= edgeThreshold);\n" +
                    "}";

    /**
     * Create a new photo-to-cartoon processor.
     * <p>
     * The initial stack of images will be empty, so <code>loadPhoto(...)</code>
     * should typically be the first method called.
     */
    public Cartoonify() {
        pixels = new int[4][];
        currImage = -1;  // no image loaded initially
    }

    /**
     * @return What level of colour change should be considered an edge.
     */
    public int getEdgeThreshold() {
        return edgeThreshold;
    }

    /**
     * Set the level of colour change that should be considered an edge.
     * Small numbers (e.g. 50) give lots of heavy black edges.
     * Large numbers (e.g. 1000) give fewer, thinner edges.
     *
     * @param edgeThreshold from 0 up to 1000 or more.
     */
    public void setEdgeThreshold(int edgeThreshold) {
        if (edgeThreshold < 0) {
            throw new IllegalArgumentException("edge threshold must be at least zero, not " + edgeThreshold);
        }
        this.edgeThreshold = edgeThreshold;
    }

    /**
     * @return Number of values in each colour channel (R, G, B) after quantization.
     */
    public int getNumColours() {
        return numColours;
    }

    /**
     * Set the number of values in each colour channel (R, G, B) after quantization.
     */
    public void setNumColours(int numColours) {
        if (0 < numColours && numColours <= 256) {
            this.numColours = numColours;
        } else {
            throw new IllegalArgumentException("NumColours must be 0..256, not " + numColours);
        }
    }

    public boolean isDebug() {
        return debug;
    }

    /**
     * Set this to true to print out extra timing information and save the intermediate photos.
     *
     * @param debug
     */
    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    /**
     * @return the number of images currently on the stack of images.
     */
    public int numImages() {
        return currImage + 1;
    }

    /**
     * Returns an internal representation of all the pixels.
     *
     * @return all the pixels in the current image that is on top of the stack.
     */
    protected int[] currentImage() {
        return pixels[currImage];
    }

    /**
     * Push the given image onto the stack of images.
     *
     * @param newPixels must be the same size (width * height pixels), and contain RGB pixels.
     */
    protected void pushImage(int[] newPixels) {
        assert newPixels.length == width * height;
        currImage++;
        if (currImage >= pixels.length) {
            // expand the maximum number of possible images.
            pixels = Arrays.copyOf(pixels, pixels.length * 2);
        }
        pixels[currImage] = newPixels;
    }

    /**
     * Remove the current image off the stack.
     *
     * @return all the pixels in that image.
     */
    protected int[] popImage() {
        final int[] result = pixels[currImage];
        pixels[currImage--] = null;
        return result;
    }

    /**
     * Push a shallow copy of the given image onto the stack.
     * For speed, this copies the pointer to the image, but does not
     * duplicate all the pixels in the image.
     * <p>
     * Negative numbers are relative to the top of the stack, so -1 means duplicate
     * the current top of the stack.  Zero or positive is relative to the bottom of
     * the stack, so 0 means duplicate the original photo.
     *
     * @param which the number of the photo to duplicate. From <code>-numImages() .. numImages()-1</code>.
     */
    public void cloneImage(int which) {
        final int stackPos = which >= 0 ? which : (currImage + which + 1);
        assert 0 <= stackPos && stackPos <= currImage;
        pushImage(Arrays.copyOf(pixels[stackPos], width * height));
    }

    /**
     * Reset the stack of images so that it is empty.
     */
    public void clear() {
        Arrays.fill(pixels, null);
        currImage = -1;
    }

    /**
     * Loads a photo from the given file.
     * <p>
     * If the stack of photos is empty, this also sets the width and height of
     * images being processed, otherwise it checks that the new image is the
     * same size as the current images.
     *
     * @param filename
     * @throws IOException if the image cannot be read or is the wrong size.
     */
    public void loadPhoto(String filename) throws IOException {
        BufferedImage image = ImageIO.read(new File(filename));
        if (image == null) {
            throw new RuntimeException("Invalid image file: " + filename);
        }
        if (numImages() == 0) {
            width = image.getWidth();
            height = image.getHeight();
        } else if (width != image.getWidth() || height != image.getHeight()) {
            throw new IOException("Incorrect image size: " + filename);
        }
        int[] newPixels = image.getRGB(0, 0, width, height, null, 0, width);
        for (int i = 0; i < newPixels.length; i++) {
            newPixels[i] &= 0x00FFFFFF; // remove any alpha channel, since we will use RGB only
        }
        pushImage(newPixels);
    }

    /**
     * Save the current photo to disk with the given filename.
     * <p>
     * Does not change the stack of images.
     *
     * @param newName the extension of this name (eg. .jpg) determines the output file type.
     * @throws IOException
     */
    public void savePhoto(String newName) throws IOException {
        BufferedImage image = new BufferedImage(width, height,
                BufferedImage.TYPE_INT_RGB);
        image.setRGB(0, 0, width, height, currentImage(), 0, width);
        final int dot = newName.lastIndexOf('.');
        final String extn = newName.substring(dot + 1);
        final File outFile = new File(newName);
        ImageIO.write(image, extn, outFile);
    }

    /**
     * @return the width of the current images that we are processing.
     */
    public int width() {
        return width;
    }

    /**
     * @return the height of the current images that we are processing.
     */
    public int height() {
        return height;
    }

    /**
     * Adds a new image that is a grayscale version of the current image.
     */
    public void grayscale() {
        int[] oldPixels = currentImage();
        int[] newPixels = new int[width * height];
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                int rgb = oldPixels[y * width + x];
                int average = (red(rgb) + green(rgb) + blue(rgb)) / 3;
                int newRGB = createPixel(average, average, average);
                newPixels[y * width + x] = newRGB;
            }
        }
        pushImage(newPixels);
    }

    public static final int[] GAUSSIAN_FILTER = {
            2, 4, 5, 4, 2, // sum=17
            4, 9, 12, 9, 4, // sum=38
            5, 12, 15, 12, 5, // sum=49
            4, 9, 12, 9, 4, // sum=38
            2, 4, 5, 4, 2  // sum=17
    };
    public static final double GAUSSIAN_SUM = 159.0;

    /**
     * Adds one new image that is a blurred version of the current image.
     */
    public void gaussianBlur() {
        long startBlur = System.currentTimeMillis();
        int[] newPixels = new int[width * height];
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                int red = clamp(convolution(x, y, GAUSSIAN_FILTER, RED) / GAUSSIAN_SUM);
                int green = clamp(convolution(x, y, GAUSSIAN_FILTER, GREEN) / GAUSSIAN_SUM);
                int blue = clamp(convolution(x, y, GAUSSIAN_FILTER, BLUE) / GAUSSIAN_SUM);
                newPixels[y * width + x] = createPixel(red, green, blue);
            }
        }
        pushImage(newPixels);
        long endBlur = System.currentTimeMillis();
        if (debug) {
            System.out.println("  gaussian blurring took " + (endBlur - startBlur) / 1e3 + " secs.");
        }
    }


    public static final int[] SOBEL_VERTICAL_FILTER = {
            -1, 0, +1,
            -2, 0, +2,
            -1, 0, +1
    };

    public static final int[] SOBEL_HORIZONTAL_FILTER = {
            +1, +2, +1,
            0, 0, 0,
            -1, -2, -1
    };

    /**
     * Detects edges in the current image and adds an image where black pixels
     * mark the edges and the other pixels are all white.
     * <p>
     * The <code>getEdgeThreshold()</code> value determines how aggressive
     * the edge-detection is.  Small values (e.g. 50) mean very aggressive,
     * while large values (e.g. 1000) generate few edges.
     */
    public void sobelEdgeDetect() {
        long startEdges = System.currentTimeMillis();
        int[] newPixels = new int[width * height];
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                int redVertical = convolution(x, y, SOBEL_VERTICAL_FILTER, RED);
                int greenVertical = convolution(x, y, SOBEL_VERTICAL_FILTER, GREEN);
                int blueVertical = convolution(x, y, SOBEL_VERTICAL_FILTER, BLUE);
                int redHorizontal = convolution(x, y, SOBEL_HORIZONTAL_FILTER, RED);
                int greenHorizontal = convolution(x, y, SOBEL_HORIZONTAL_FILTER, GREEN);
                int blueHorizontal = convolution(x, y, SOBEL_HORIZONTAL_FILTER, BLUE);
                int verticalGradient = Math.abs(redVertical) + Math.abs(greenVertical) + Math.abs(blueVertical);
                int horizontalGradient = Math.abs(redHorizontal) + Math.abs(greenHorizontal) + Math.abs(blueHorizontal);
                // we could take use sqrt(vertGrad^2 + horizGrad^2), but simple addition catches most edges.
                int totalGradient = verticalGradient + horizontalGradient;
                if (totalGradient >= edgeThreshold) {
                    newPixels[y * width + x] = black; // we colour the edges black
                } else {
                    newPixels[y * width + x] = white;
                }
            }
        }
        pushImage(newPixels);
        long endEdges = System.currentTimeMillis();
        if (debug) {
            System.out.println("  sobel edge detect took " + (endEdges - startEdges) / 1e3 + " secs.");
        }
    }

    /**
     * Adds a new image that is the same as the current image but with fewer colours.
     * <p>
     * The <code>getNumColours()</code> setting determines the desired number of
     * colour values in EACH colour channel after this method finishes.
     */
    public void reduceColours() {
        long startQuantize = System.currentTimeMillis();
        int[] oldPixels = currentImage();
        int[] newPixels = new int[width * height];
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                int rgb = oldPixels[y * width + x];
                int newRed = quantizeColour(red(rgb), numColours);
                int newGreen = quantizeColour(green(rgb), numColours);
                int newBlue = quantizeColour(blue(rgb), numColours);
                int newRGB = createPixel(newRed, newGreen, newBlue);
                newPixels[y * width + x] = newRGB;
            }
        }
        pushImage(newPixels);
        long endQuantize = System.currentTimeMillis();
        if (debug) {
            System.out.println("  colour reduction took  " + (endQuantize - startQuantize) / 1e3 + " secs.");
        }
    }

    /**
     * Converts the given colour value (eg. 0..255) to an approximate colour value.
     * This is a helper method for reducing the number of colours in the image.
     * <p>
     * For example, if numPerChannel is 3, then:
     * <ul>
     * <li>0..85 will be mapped to 0;</li>
     * <li>86..170 will be mapped to 127;</li>
     * <li>171..255 will be mapped to 255;</li>
     * </ul>
     * So the output colour values always start at 0, end at COLOUR_MASK, and any other
     * values are spread out evenly in between.  This requires some careful maths, to
     * avoid overflow and to divide the input colours up into <code>numPerChannel</code>
     * equal-sized buckets.
     *
     * @param colourValue   0 .. COLOUR_MASK
     * @param numPerChannel how many colours we want in the output.
     * @return a discrete colour value (0..COLOUR_MASK).
     */
    int quantizeColour(int colourValue, int numPerChannel) {
        float colour = colourValue / (COLOUR_MASK + 1.0f) * numPerChannel;
        int discrete = Math.round(colour - 0.5f);
        assert 0 <= discrete && discrete < numPerChannel;
        int newColour = discrete * COLOUR_MASK / (numPerChannel - 1);
        assert 0 <= newColour && newColour <= COLOUR_MASK;
        return newColour;
    }

    /**
     * Merges a mask image on top of another image.
     * <p>
     * Since this operation takes two input images, it allows the caller
     * to specify those images by their position.  The input images are
     * left unchanged, and the new merged image is pushed on top of the stack.
     *
     * @param maskImage  the number/position of the mask (as for cloneImage).
     * @param maskColour an exact pixel colour.  Where the mask is this colour,
     *                   the other image will be chosen.
     * @param otherImage the number/position of the underneath image.
     */
    public void mergeMask(int maskImage, int maskColour, int otherImage) {
        long startMasking = System.currentTimeMillis();
        cloneImage(maskImage);
        int[] maskPixels = popImage();
        cloneImage(otherImage);
        int[] photoPixels = popImage();
        int[] newPixels = new int[width * height];
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                int index = y * width + x;
                if (maskPixels[index] == maskColour) {
                    newPixels[index] = photoPixels[index];
                } else {
                    newPixels[index] = maskPixels[index];
                }
            }
        }
        pushImage(newPixels);
        long endMasking = System.currentTimeMillis();
        if (debug) {
            System.out.println("  masking edges took     " + (endMasking - startMasking) / 1e3 + " secs.");
        }
    }

    /**
     * This applies the given N*N filter around the pixel (xCentre,yCentre).
     * <p>
     * Applying a filter means multiplying each nearby pixel (within the N*N box)
     * by the corresponding factor in the filter array (which is conceptually a 2D matrix).
     * <p>
     * This method does not change the current image at all.  It just multiplies
     * the filter matrix by the colour values of the pixels around (xCentre,yCentre)
     * and returns the resulting (integer) value.
     * <p>
     * This method is 'package-private' (the default protection) so that the tests can test it.
     *
     * @param xCentre
     * @param yCentre
     * @param filter  a 2D square matrix, laid out in row-major order in a 1D array.
     * @param colour  which colour to apply the filter to.
     * @return the sum of multiplying the requested colour of each pixel by its filter factor.
     */
    int convolution(int xCentre, int yCentre, int[] filter, int colour) {
        int sum = 0;
        // find the width and height of the filter matrix, which must be square.
        int filterSize = 1;
        while (filterSize * filterSize < filter.length) {
            filterSize++;
        }
        if (filterSize * filterSize != filter.length) {
            throw new IllegalArgumentException("non-square filter: " + Arrays.toString(filter));
        }
        final int filterHalf = filterSize / 2;

        /**
         * Unroll the for-loops
         */
        if (filterSize == 3) {
            int y0 = wrap(yCentre - 1, height);
            int y1 = wrap(yCentre, height);
            int y2 = wrap(yCentre + 1, height);
            int x0 = wrap(xCentre - 1, width);
            int x1 = wrap(xCentre, width);
            int x2 = wrap(xCentre + 1, width);

            int rgb = pixel(x0, y0);
            sum += colourValue(rgb, colour) * filter[0];

            rgb = pixel(x1, y0);
            sum += colourValue(rgb, colour) * filter[1];

            rgb = pixel(x2, y0);
            sum += colourValue(rgb, colour) * filter[2];

            rgb = pixel(x0, y1);
            sum += colourValue(rgb, colour) * filter[3];

            rgb = pixel(x1, y1);
            sum += colourValue(rgb, colour) * filter[4];

            rgb = pixel(x2, y1);
            sum += colourValue(rgb, colour) * filter[5];

            rgb = pixel(x0, y2);
            sum += colourValue(rgb, colour) * filter[6];

            rgb = pixel(x1, y2);
            sum += colourValue(rgb, colour) * filter[7];

            rgb = pixel(x2, y2);
            sum += colourValue(rgb, colour) * filter[8];
        } else if (filterSize == 5) {
            int y0 = wrap(yCentre - 2, height);
            int y1 = wrap(yCentre - 1, height);
            int y2 = wrap(yCentre, height);
            int y3 = wrap(yCentre + 1, height);
            int y4 = wrap(yCentre + 2, height);
            int x0 = wrap(xCentre - 2, width);
            int x1 = wrap(xCentre - 1, width);
            int x2 = wrap(xCentre, width);
            int x3 = wrap(xCentre + 1, width);
            int x4 = wrap(xCentre + 2, width);

            int rgb = pixel(x0, y0);
            sum += colourValue(rgb, colour) * filter[0];

            rgb = pixel(x1, y0);
            sum += colourValue(rgb, colour) * filter[1];

            rgb = pixel(x2, y0);
            sum += colourValue(rgb, colour) * filter[2];

            rgb = pixel(x3, y0);
            sum += colourValue(rgb, colour) * filter[3];

            rgb = pixel(x4, y0);
            sum += colourValue(rgb, colour) * filter[4];

            rgb = pixel(x0, y1);
            sum += colourValue(rgb, colour) * filter[5];

            rgb = pixel(x1, y1);
            sum += colourValue(rgb, colour) * filter[6];

            rgb = pixel(x2, y1);
            sum += colourValue(rgb, colour) * filter[7];

            rgb = pixel(x3, y1);
            sum += colourValue(rgb, colour) * filter[8];

            rgb = pixel(x4, y1);
            sum += colourValue(rgb, colour) * filter[9];

            rgb = pixel(x0, y2);
            sum += colourValue(rgb, colour) * filter[10];

            rgb = pixel(x1, y2);
            sum += colourValue(rgb, colour) * filter[11];

            rgb = pixel(x2, y2);
            sum += colourValue(rgb, colour) * filter[12];

            rgb = pixel(x3, y2);
            sum += colourValue(rgb, colour) * filter[13];

            rgb = pixel(x4, y2);
            sum += colourValue(rgb, colour) * filter[14];

            rgb = pixel(x0, y3);
            sum += colourValue(rgb, colour) * filter[15];

            rgb = pixel(x1, y3);
            sum += colourValue(rgb, colour) * filter[16];

            rgb = pixel(x2, y3);
            sum += colourValue(rgb, colour) * filter[17];

            rgb = pixel(x3, y3);
            sum += colourValue(rgb, colour) * filter[18];

            rgb = pixel(x4, y3);
            sum += colourValue(rgb, colour) * filter[19];

            rgb = pixel(x0, y4);
            sum += colourValue(rgb, colour) * filter[20];

            rgb = pixel(x1, y4);
            sum += colourValue(rgb, colour) * filter[21];

            rgb = pixel(x2, y4);
            sum += colourValue(rgb, colour) * filter[22];

            rgb = pixel(x3, y4);
            sum += colourValue(rgb, colour) * filter[23];

            rgb = pixel(x4, y4);
            sum += colourValue(rgb, colour) * filter[24];
        } else {
            for (int filterY = 0; filterY < filterSize; filterY++) {
                int y = wrap(yCentre + filterY - filterHalf, height);
                for (int filterX = 0; filterX < filterSize; filterX++) {
                    int x = wrap(xCentre + filterX - filterHalf, width);
                    int rgb = pixel(x, y);
                    int filterVal = filter[filterY * filterSize + filterX];
                    sum += colourValue(rgb, colour) * filterVal;
                }
            }
        }
//         System.out.println("convolution(" + xCentre + ", " + yCentre + ") = " + sum);
        return sum;
    }

    /**
     * Restricts an index to be within the image.
     * <p>
     * Different strategies are possible for this, such as wrapping around,
     * clamping to 0 and size-1, or reflecting off the edge.
     * <p>
     * The current implementation reflects off each edge.
     *
     * @param pos  an index that might be slightly outside the image boundaries.
     * @param size the width of the image (for x value) or the height (for y values).
     * @return the new index, which is in the range <code>0 .. size-1</code>.
     */
    public int wrap(int pos, int size) {
        /*Original implementation*/
//        if (pos < 0) {
//            pos = -1 - pos;
//        } else if (pos >= size) {
//            pos = (size - 1) - (pos - size);
//        }
//        assert 0 <= pos;
//        assert pos < size;
//        return pos;

        /**
         * Alternative wrap method
         */
        return pos < 0 ? 0 : pos >= size ? size - 1 : pos;
    }

    /**
     * Clamp a colour value to be within the allowable range for each colour.
     *
     * @param value a floating point colour value, which may be out of range.
     * @return an integer colour value, in the range <code>0 .. COLOUR_MASK</code>.
     */
    public int clamp(double value) {
//        int result = (int) (value + 0.5); // round to nearest integer
        int result = (int)value;
        if (result <= 0) {
            return 0;
        } else if (result > COLOUR_MASK) {
            return 255;
        } else {
            return result;
        }
    }

    /**
     * Get a pixel from within the current photo.
     *
     * @param x must be in the range <code>0 .. width-1</code>.
     * @param y must be in the range <code>0 .. height-1</code>.
     * @return the requested pixel of the current image, in RGB format.
     * @throws ArrayIndexOutOfBoundsException exception if there is no current image.
     */
    public int pixel(int x, int y) throws ArrayIndexOutOfBoundsException {
        return currentImage()[y * width + x];
    }

    /**
     * Extract a given colour channel out of the given pixel.
     *
     * @param pixel  an RGB value.
     * @param colour one of RED, GREEN or BLUE.
     * @return a colour value, ranging from 0 .. COLOUR_MASK.
     */
    public final int colourValue(int pixel, int colour) {
        return (pixel >> (colour * COLOUR_BITS)) & COLOUR_MASK;
    }

    /**
     * Get the red value of the given pixel.
     *
     * @param pixel an RGB value.
     * @return a value in the range 0 .. COLOUR_MASK
     */
    public final int red(int pixel) {
        return colourValue(pixel, RED);
    }

    /**
     * Get the green value of the given pixel.
     *
     * @param pixel an RGB value.
     * @return a value in the range 0 .. COLOUR_MASK
     */
    public final int green(int pixel) {
        return colourValue(pixel, GREEN);
    }

    /**
     * Get the blue value of the given pixel.
     *
     * @param pixel an RGB value.
     * @return a value in the range 0 .. COLOUR_MASK
     */
    public final int blue(int pixel) {
        return colourValue(pixel, BLUE);
    }

    /**
     * Constructs one integer RGB pixel from the individual components.
     *
     * @param redValue
     * @param greenValue
     * @param blueValue
     * @return
     */
    public final int createPixel(int redValue, int greenValue, int blueValue) {
        assert 0 <= redValue && redValue <= COLOUR_MASK;
        assert 0 <= greenValue && greenValue <= COLOUR_MASK;
        assert 0 <= blueValue && blueValue <= COLOUR_MASK;
        return (redValue << (2 * COLOUR_BITS)) + (greenValue << COLOUR_BITS) + blueValue;
    }

    /**
     * Processes one input photo, applying all the desired transformations to it.
     * Saves the resulting photo in a new file of the same type.
     * E.g. if the input file is "foo.jpg" the output file will be "foo_cartoon.jpg".
     *
     * @param name path to the photo, including a known extension (e.g. ".jpg").
     * @return the number of milliseconds to process this photo (excluding loading/saving).
     * @throws IOException
     */
    protected long processPhoto(String name) throws IOException {
        //no need to change the implementation of this method
        int dot = name.lastIndexOf(".");
        if (dot <= 0) {
            System.err.println("Skipping unknown kind of file: " + name);
            return 0L;
        }
        final String baseName = name.substring(0, dot);
        final String extn = name.substring(dot).toLowerCase();
        loadPhoto(name);
        final String newName = baseName + "_cartoon" + extn;
        //Please do NOT change the start of time measurement
        final long time0 = System.currentTimeMillis();
        if (useGPU) {
            processPhotoOpenCL();
        } else {
            processPhotoOnCPU();
        }
        //Please do NOT change the end of time measurement
        long time1 = System.currentTimeMillis();
        //Please do NOT remove or change this output message
        System.out.println("Done " + name + " -> " + newName + " in " + (time1 - time0) / 1e3 + " secs.");
        savePhoto(newName);
        if (debug) {
            // At this stage the stack of images is (from bottom to top):
            //  original, blurred, edges, original, quantized, final
            popImage();
            savePhoto(baseName + "_colours" + extn);
            popImage();
            popImage();
            savePhoto(baseName + "_edges" + extn);
            popImage();
            savePhoto(baseName + "_blurred" + extn);
            popImage();
            assert numImages() == 1;
        }
        clear();
        return time1 - time0;
    }


    /**
     * Implement this method to process one input photo on GPU or GPU and CPU
     */
    protected void processPhotoOpenCL() {
        gaussianBlur_sobelEdgeDetect_OpenCL();
        int edgeMask = numImages() - 1;
        // now convert the original image into a few discrete colours
        cloneImage(0);
        reduceColours();
        mergeMask(edgeMask, white, -1);
    }

    /**
     * GPU version:
     *
     *  Adds one new image that is a blurred version of the current image.
     *
     *  Detects edges in the current image and adds an image where black pixels
     *  mark the edges and the other pixels are all white.
     */
    private void gaussianBlur_sobelEdgeDetect_OpenCL() {
        long startBlur = System.currentTimeMillis();

        final int length = width * height;
        final int workgroupsize = 64; //the preferred group size multiple on my machine

        //Create an OpenCL queue on the first device of this context.
        CLQueue queue1 = context.createDefaultQueue();
        CLQueue queue2 = context.createDefaultQueue();

        // Allocate OpenCL-hosted memory
        CLBuffer<Integer> memIn = context.createIntBuffer(CLMem.Usage.Input, length);
        CLBuffer<Integer> memInOut = context.createIntBuffer(CLMem.Usage.InputOutput, length);
        CLBuffer<Integer> memOut = context.createIntBuffer(CLMem.Usage.Output, length);


        // Map input buffers to populate them with some data
        Pointer<Integer> a = memIn.map(queue1, CLMem.MapFlags.Write);

        //Fill the arrays with image
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                a.setIntAtIndex(y * width + x, pixel(x, y));
            }
        }

        //Unmap input buffers
        memIn.unmap(queue1, a);

        CLProgram program = context.createProgram(source).build();
        CLKernel blurKernel = program.createKernel("gaussianBlur", memIn, memInOut, width, height);
        CLKernel edgeKernel = program.createKernel("sobelEdgeDetect", memInOut, memOut, width, height, edgeThreshold);

        CLEvent blurEvent = blurKernel.enqueueNDRange(queue1, new int[]{length}, new int[]{workgroupsize});
        CLEvent edgeEvent = edgeKernel.enqueueNDRange(queue2, new int[]{length}, new int[]{workgroupsize}, blurEvent);

        //Execution begins when the user executes a synchronizing command,
        queue2.flush();
        queue1.flush();

        // Wait for all operations to be performed
        queue1.finish();
        queue2.finish();

        Pointer<Integer> output1 = memInOut.read(queue1);
        Pointer<Integer> output2 = memOut.read(queue2);

        pushImage(output1.getInts());
        pushImage(output2.getInts());
        long endBlur = System.currentTimeMillis();
        if (debug) {
            System.out.println("  gaussian blurring and sobel Edge Detect took " + (endBlur - startBlur) / 1e3 + " secs.");
        }
    }


    /**
     * Process one input photo step-by-step on CPU
     */
    protected void processPhotoOnCPU() {
        // no need to change the implementation of this method
        // This sequence of processing commands is done to every photo.
        gaussianBlur();
        sobelEdgeDetect();
        int edgeMask = numImages() - 1;
        // now convert the original image into a few discrete colours
        cloneImage(0);
        reduceColours();
        mergeMask(edgeMask, white, -1);
    }


    /**
     * Uses the given command line arguments to set Cartoonify options.
     *
     * @param args     command line arguments
     * @param firstArg the first argument to start at.
     * @return the position of the first non-flag argument.  That is, first file.
     */
    protected int setFlags(String[] args, int firstArg) {
        int currArg = firstArg;
        if ("-g".equals(args[currArg])) {
            useGPU = true;
            currArg += 1;
        }
        if ("-d".equals(args[currArg])) {
            setDebug(true);
            currArg += 1;
        }
        if ("-e".equals(args[currArg])) {
            setEdgeThreshold(Integer.parseInt(args[currArg + 1]));
            System.out.println("Using edge threshold " + getEdgeThreshold());
            currArg += 2;
        }
        if ("-c".equals(args[currArg])) {
            setNumColours(Integer.parseInt(args[currArg + 1]));
            System.out.println("Using " + getNumColours() + " discrete colours per channel.");
            currArg += 2;
        }
        return currArg;
    }

    /**
     * Prints a help/usage message to standard output.
     */
    public void help() {
        System.out.println("Arguments: [-g] [-d] [-e EdgeThreshold] [-c NumColours] photo1.jpg photo2.jpg ...");
        System.out.println("  -g use the GPU, to speed up photo processing.");
        System.out.println("  -d means turn on debugging, which saves intermediate photos.");
        System.out.println("  -e EdgeThreshold values can range from 0 (everything is an edge) up to about 1000 or more.");
        System.out.println("  -c NumColours is the number of discrete values within each colour channel (2..256).");
    }

    /**
     * Run this with no arguments to see the usage message.
     *
     * @param args command line arguments
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        //no need to change the implementation of the main method
        Cartoonify cartoon = new Cartoonify();
        if (args.length == 0) {
            cartoon.help();
            System.exit(1);
        }
        int arg = cartoon.setFlags(args, 0);
        long time = 0;
        int done = 0;
        for (; arg < args.length; arg++) {
            time += cartoon.processPhoto(args[arg]);
            done++;
        }
        //Please do NOT remove or change this output message
        System.out.format("Average processing time is %.3f for %d photos.", time / done / 1e3, done);
    }

}
