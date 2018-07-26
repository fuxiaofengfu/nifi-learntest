package other;

import java.io.*;
import java.util.stream.Stream;

public class FinalTest {

	public final AvroUtil avroUtil = new AvroUtil();

	public AvroUtil avroUtil1 = new AvroUtil();

	public static void main(String[] args) throws IOException {
		FileInputStream in = new FileInputStream("cpu.txt");
		BufferedReader d = new BufferedReader(new InputStreamReader(in));

		Stream<String> lines = d.lines();

		int cpuGroupLine = 9; // 一组cpu的行数
		int cpuPosition = 8;
		int memPostition = 9;
		lines.forEach(line ->{
			System.out.println(line.trim().replaceAll("\\s+"," "));
		});
		String ss = "1590 hadoop    20   0 2976256  98892  13404 S   0.0  5.3   0:32.03 java";
		ss = ss.replaceAll("\\s+"," ");
		String[] split = ss.split(" ");

		System.out.println(split[8]);
		System.out.println(split[9]);

	}
}
