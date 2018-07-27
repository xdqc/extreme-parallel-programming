package pi;

import org.bridj.Pointer;

import com.nativelibs4java.opencl.CLBuffer;
import com.nativelibs4java.opencl.CLContext;
import com.nativelibs4java.opencl.CLDevice;
import com.nativelibs4java.opencl.CLPlatform.DeviceFeature;
import com.nativelibs4java.opencl.CLKernel;
import com.nativelibs4java.opencl.CLMem.MapFlags;
import com.nativelibs4java.opencl.CLMem.Usage;
import com.nativelibs4java.opencl.CLPlatform;
import com.nativelibs4java.opencl.CLProgram;
import com.nativelibs4java.opencl.CLQueue;
import com.nativelibs4java.opencl.JavaCL;

import java.util.Arrays;

/**
 * Simple JavaCL example that estimates PI by firing darts at a dart board.
 *
 * This is based on the HADOOP PI estimation example.
 *
 * This uses JavaCL 1.0.0-SNAPSHOT, which uses the new BridJ API,
 * with the generic buffers, like CLBuffer&lt;Integer&gt;.
 *
 * @author Mark Utting
 */
public class Pi {

	/**
	 * A dummy Java-version of our kernel. This is useful so that we can test
	 * and debug it in Java first.
	 * 
	 * @param seeds
	 *            one integer seed for each thread (work item).
	 * @param repeats
	 *            the number of darts each thread must throw.
	 * @param output
	 *            one integer output cell for each thread
	 * @param gid
	 *            dummy global id, only needed in the Java API, not the OpenCL
	 *            version. (delete this parameter when you translate this to an
	 *            OpenCL kernel).
	 */
	public static void dummyThrowDarts(int[] seeds, int repeats, int[] output,
			int gid) {
		// int gid = get_global_id(0); // this is how we get the gid in OpenCL.
		int rand = seeds[gid];
		for (int iter = 0; iter < repeats; iter++) {
            for (int i = 0; i < repeats; i++) {
                rand = 1103515245 * rand + 12345;	// Linear congruential generator
                float x = ((float) (rand & 0xffffff)) / 0x1000000; //convert the resulting random integer into a floating point number between 0.0 and 1.0

                rand = 1103515245 * rand + 12345;
                float y = ((float) (rand & 0xffffff)) / 0x1000000;

                output[gid] += x*x+y*y < 1 ? 1 : 0;
            }
        }
	}

	public static void dummyThrowDartsInt(int[] seeds, int repeats, int[] output,
									   int gid) {
		// int gid = get_global_id(0); // this is how we get the gid in OpenCL.
		int rand = seeds[gid];
		for (int iter = 0; iter < repeats; iter++) {
			for (int i = 0; i < repeats; i++) {
				rand = 1103515245 * rand + 12345;	// generate random integer
				int x = rand & 0x3fff;				// make sure x or y is smaller than 0x4000, so that x*x < 0x100000000, to prevent integer over flow
				rand = 1103515245 * rand + 12345;
				int y = rand & 0x3fff;
				output[gid] += x*x+y*y < 0xfff8001  ? 1 : 0;
			}
		}
	}

	/**
	 * main arguments: threads workgroupsize darts_per_thread
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		/**
		 * Test dummyThrowDartsInt
		 */
//		int[] results = new int[6];
//		dummyThrowDartsInt(new int[] {0,1,2,3,4,5}, 1000, results, 0);
//		System.out.println(Arrays.toString(results));


		if (args.length != 3) {
			System.err.println("Usage: pi threads workgroupsize repeats");
			System.err.println("      (threads must be a multiple of workgroupsize)");
			System.exit(1);
		}
		final int threads = Integer.decode(args[0]);
		final int wgSize = Integer.decode(args[1]);
		final int repeats = Integer.decode(args[2]);
		// we can list all available platforms and devices.
		for (CLPlatform p : JavaCL.listPlatforms()) {
			System.out.println("CLPlatform: " + p.getName() + " from "
					+ p.getVendor());
			for (CLDevice dev : p.listAllDevices(false)) {
				System.out.println("  CLDevice: " + dev.getName() + " has "
						+ dev.getMaxComputeUnits() + " compute units");
                System.out.println("  CLDevice: " + dev.getName() + " has "
                        + dev.getMaxWorkGroupSize() + " workgroups");
			}
		}

		// choose the platform and device with the most compute units
		// CLContext context = JavaCL.createBestContext();
		// or make sure GPU is usedSNAPSHOT
		CLContext context = JavaCL.createBestContext(DeviceFeature.GPU);
	
		System.out.println("best context has device[0]=" + context.getDevices()[0]);

		CLQueue queue = context.createDefaultQueue();

		// Allocate OpenCL-hosted memory for inputs and output
		CLBuffer<Integer> memIn1 = context
				.createIntBuffer(Usage.Input, threads);
		CLBuffer<Integer> memOut = context.createIntBuffer(Usage.Output, threads);

		// Map input buffers to populate them with some data
		Pointer<Integer> a = memIn1.map(queue, MapFlags.Write);
		// Fill the mapped input buffers with random seeds: 0 .. threads-1
		int seed = (int)System.currentTimeMillis() & 0x000FFFFF;
		for (int i = 0; i < threads; i++) {
			a.setIntAtIndex(i,i*seed);
		}
		// Unmap input buffers
		memIn1.unmap(queue, a);

        /**
         * Translate kernel C code from dummyThrowDarts
         *
         * As image objects(e.g. int repeat) are always allocated from the global address space,
         * the __global or global qualifier should not be specified for image types.
         */
		String throwDartsFloat = "__kernel void throwDarts(" +
                "__global const int *seed," +
                "const int repeat," +
                "__global int *output)" +
                "{" +
                "   int gid = get_global_id(0);" +
				"   int rand = seed[gid];" +
				"	output[gid] = 0;" +
				"   for (int i = 0; i < repeat; i++) {" +
                "       rand = 1103515245 * rand + 12345;" +
                "       float x = ((float) (rand & 0xffffff)) / 0x1000000;" +
                "       rand = 1103515245 * rand + 12345;" +
                "       float y = ((float) (rand & 0xffffff)) / 0x1000000;" +
                "       output[gid] += x*x+y*y < 1 ? 1 : 0;" +
                "   }" +
                "}";

		/**
		 * Translate kernel C code from dummyThrowDartsInt
		 *
		 * make sure x or y is smaller than 0x4000, so that x*x < 0x10000000
		 * to prevent positive integer overflow to become negative integer (2's complement number)
		 */
		String throwDartsInt = "__kernel void throwDarts(" +
				"__global const int *seed," +
				"const int repeat," +
				"__global int *output)" +
				"{" +
				"   int gid = get_global_id(0);" +
				"   int rand = seed[gid];" +
				"	output[gid] = 0;" +
				"   for (int i = 0; i < repeat; i++) {" +
				"       rand = 1103515245 * rand + 12345;" +
				"       int x = rand & 0x3fff;" +
				"       rand = 1103515245 * rand + 12345;" +
				"       int y = rand & 0x3fff;" +
				"       output[gid] += x*x+y*y < 0xfff8001 ? 1 : 0;" +
				"   }" +
				"}";


		CLProgram program = context.createProgram(throwDartsFloat).build();
		CLKernel kernel = program.createKernel("throwDarts", memIn1, repeats, memOut);

		// Execute the kernel with global size = dataSize 
		// and workgroup size = wgSize
		System.out.println("Starting with " + threads + " threads, each doing "
				+ repeats + " repeats.");
		System.out.flush();
		final long time0 = System.nanoTime();
		kernel.enqueueNDRange(queue, new int[] { threads },	new int[] { wgSize });

		// Wait for all operations to be performed
		queue.finish();
		final long time1 = System.nanoTime();
		System.out.println("Done in " + (time1 - time0) / 1000
				+ " microseconds");

		// Copy the OpenCL-hosted output array back to RAM
		// We could do this via map;take-local-copy;unmap, but read does all
		// that for us.
		Pointer<Integer> output = memOut.read(queue);

		// Analyze the results and calculate PI
		long inside = 0;
		long total = (long) threads * repeats;
		for (int i = 0; i < threads; i++) {
			// System.out.println("thread i: " + i + " gives " + output.get(i));
			inside += output.get(i);
		}
		final double pi = 4.0 * inside / total;
		System.out.println("Estimate PI = " + inside + "/" + total + " = " + pi);
	}
}
