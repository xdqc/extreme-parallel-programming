package nz.ac.waikato.phototool;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;
import javax.swing.ImageIcon;
import javax.swing.JFrame;
import javax.swing.JLabel;

/**
 * Maintains a 2D matrix of RGB pixels, for image processing.
 * <p>
 * It also provides methods for reading the pixels from an image file,
 * writing the pixels to an image file,
 * and displaying the image in a popup window.
 *
 * @author Mark Utting
 */
class Picture {
    private int width;
    private int height;
    private int[] pixels;    // Task 2.2 Go primitive in your arrays!

    public Picture(String filename) {
        BufferedImage image;
        try {
            image = ImageIO.read(new File(filename));
        } catch (IOException ex) {
            throw new RuntimeException("Could not open file: " + filename + ": " + ex.getMessage());
        }
        if (image == null) {
            throw new RuntimeException("Invalid image file: " + filename);
        }
        width = image.getWidth();
        height = image.getHeight();
        pixels = image.getRGB(0, 0, width, height, null, 0, width);
    }

    public Picture(int w, int h) {
        width = w;
        height = h;
        pixels = new int[width * height]; // Task 2.2 Go primitive in your arrays!

        // Task 2.4 Find the bottlenecks!
        // The constructor of Picture class is the most heavily used complied method (via -Xprof)
        // Time is wasted at unnecessary initializing 2-D arrays
    }

    public int width() {
        return width;
    }

    public int height() {
        return height;
    }

    public int get(int x, int y) {
        return pixels[x + y * width]; // Task 2.2 Go primitive in your arrays!
    }

    public void set(int x, int y, int newPixel) {
        pixels[x + y * width] = newPixel;
    }

    private BufferedImage getImage() {
        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                image.setRGB(x, y, get(x, y));
            }
        }
        return image;
    }

    public void show(String name) {
        JFrame frame = new JFrame();
        ImageIcon icon = new ImageIcon(getImage());
        frame.setContentPane(new JLabel(icon));
        frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        frame.setTitle(name);
        frame.setResizable(false);
        frame.pack();
        frame.setVisible(true);
    }

    public void save(String newName) throws IOException {
        BufferedImage image = getImage();
        final int dot = newName.lastIndexOf('.');
        final String extn = newName.substring(dot + 1);
        final File outFile = new File(newName);
        ImageIO.write(image, extn, outFile);
    }
}