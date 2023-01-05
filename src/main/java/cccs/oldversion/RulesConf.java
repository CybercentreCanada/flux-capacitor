package cccs.oldversion;

import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class RulesConf {
	public List<RuleConf> rules;

	public static class TagConf {
		public String name;
	}

	public static class RuleConf {
		public String rulename;
		public String description;
		public String action;
		public List<TagConf> tags;
		public List<String> groupby;
		public boolean ordered;
		public String parent;
		public String child;
	}

	public static RulesConf load(String spec) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        objectMapper.findAndRegisterModules();
        return objectMapper.readValue(spec, RulesConf.class);
    }
}
