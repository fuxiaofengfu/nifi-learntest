package nifitest;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class NifiTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	public NifiTest() {
	}

	@Test
	public void testReplacementWithExpressionLanguage() throws IOException {
		TestRunner runner = TestRunners.newTestRunner(new ReplaceText());
		runner.setValidateExpressionUsage(false);
		runner.setProperty(ReplaceText.SEARCH_VALUE, "${replaceKey}");
		runner.setProperty(ReplaceText.REPLACEMENT_VALUE, "GoodBye");
		Map<String, String> attributes = new HashMap();
		attributes.put("replaceKey", "H.*o");
		runner.enqueue(Paths.get("src/test/resources/hello.txt"), attributes);

		runner.run();
		runner.assertAllFlowFilesTransferred(ReplaceText.REL_SUCCESS, 1);

		MockFlowFile out = runner.getFlowFilesForRelationship(ReplaceText.REL_SUCCESS).get(0);
		out.assertContentEquals("Hello, World!");

		out.getAttributes().forEach((k, v) -> {
			System.out.println(k + "===" + v);
		});
		ProcessContext processContext = runner.getProcessContext();
		String value = processContext.getProperty(ReplaceText.SEARCH_VALUE).evaluateAttributeExpressions(out).getValue();
		System.out.println(value);
		System.out.println("flowFile uuid attr ==" + out.getAttribute("uuid"));


	}
}
