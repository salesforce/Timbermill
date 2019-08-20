package com.datorama.timbermill.plugins;

import com.datorama.timbermill.unit.Event;

import java.io.Serializable;
import java.util.List;

public class SwitchCasePlugin extends TaskLogPlugin {

	private static final long serialVersionUID = 1154980374224187763L;
	private final TaskMatcher taskMatcher;
	private final String searchField;
	private final String outputAttribute;
	private final List<CaseRule> switchCase;

	public SwitchCasePlugin(String name, TaskMatcher taskMatcher, String searchField, String outputAttribute, List<CaseRule> switchCase) {
		super(name);
		this.taskMatcher = taskMatcher;
		this.searchField = searchField;
		this.outputAttribute = outputAttribute;
		this.switchCase = switchCase;
	}

	@Override
	public void apply(List<Event> events) {
		events.stream()
			.filter(e -> e.getTexts().containsKey(searchField))
			.filter(e -> taskMatcher.matches(e))
			.forEach(t -> {
				String searchText = t.getTexts().get(searchField);
				if (searchText != null) {
					for (CaseRule caseRule : switchCase) {
						if (caseRule.matches(searchText)) {
							t.getStrings().put(outputAttribute, caseRule.getOutput());
							break;
						}
					}
				}
			});
	}

	public static class CaseRule implements Serializable{
		private static final long serialVersionUID = 642071029855386301L;

		private List<String> match;
		private String output;

		public CaseRule() {
		}

		public CaseRule(List<String> match, String output) {
			this.match = match;
			this.output = output;
		}

		boolean matches(String searchText) {
			if (match == null) {
				return true;
			}

			for (String searchPattern : match) {
				if (searchText.contains(searchPattern)) {
					return true;
				}
			}
			return false;
		}

		String getOutput() {
			return output;
		}

	}

}
