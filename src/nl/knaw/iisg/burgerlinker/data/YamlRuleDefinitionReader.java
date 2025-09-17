package nl.knaw.iisg.burgerlinker.data;


import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;
import org.jeasy.rules.support.reader.AbstractRuleDefinitionReader;


@SuppressWarnings("unchecked")
public class YamlRuleDefinitionReader extends AbstractRuleDefinitionReader {
    /**
     * Modified version of Easy Ules YamlRuleDefinitionReader that ensures
     * valid rules despite no actions given in the YAML file.
     */

    private final Yaml yaml;

    /**
     * Create a new {@link YamlRuleDefinitionReader}.
     */
    public YamlRuleDefinitionReader() {
        this(new Yaml());
    }

    /**
     * Create a new {@link YamlRuleDefinitionReader}.
     *
     * @param yaml to use to read rule definitions
     */
    public YamlRuleDefinitionReader(Yaml yaml) {
        this.yaml = yaml;
    }

    @Override
    protected Iterable<Map<String, Object>> loadRules(Reader reader) {
        List<Map<String, Object>> rulesList = new ArrayList<>();
        Iterable<Object> rules = yaml.loadAll(reader);
        for (Object rule : rules) {
            List<String> actions = new ArrayList<>();
            actions.add(";");

            ((Map<String, Object>)rule).put("actions", (Object) actions);

            rulesList.add((Map<String, Object>) rule);
        }

        return rulesList;
    }
}
