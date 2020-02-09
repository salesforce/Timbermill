package com.datorama.timbermill.plugins;

import com.datorama.timbermill.unit.Event;
import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.Collection;

public class SwitchCasePlugin extends TaskLogPlugin {

	private static final long serialVersionUID = 1154980374224187763L;
	private final TaskMatcher taskMatcher;
	private final String searchField;
	private final String outputAttribute;
	private final Collection<CaseRule> switchCase;

	public SwitchCasePlugin(String name, TaskMatcher taskMatcher, String searchField, String outputAttribute, Collection<CaseRule> switchCase) {
		super(name);
		this.taskMatcher = taskMatcher;
		this.searchField = searchField;
		this.outputAttribute = outputAttribute;
		this.switchCase = switchCase;
	}

	@Override
	public void apply(Collection<Event> events) {
		events.stream()
			.filter(e -> e.getText() != null && e.getText().containsKey(searchField))
			.filter(e -> taskMatcher.matches(e))
			.forEach(e -> {
				String searchText = e.getText().get(searchField);
				if (searchText != null) {
					for (CaseRule caseRule : switchCase) {
						if (caseRule.matches(searchText)) {
							if (e.getStrings() == null){
								e.setStrings(Maps.newHashMap());
							}
							e.getStrings().put(outputAttribute, caseRule.getOutput());
							break;
						}
					}
				}
			});
	}

	public static class CaseRule implements Serializable{
		private static final long serialVersionUID = 642071029855386301L;

		private Collection<String> match;
		private String output;

		public CaseRule() {
		}

		public CaseRule(Collection<String> match, String output) {
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
