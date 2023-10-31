package timbermill

type TimberParams struct {
	strings map[string]string
	text    map[string]string
	context map[string]string
	metrics map[string]int
}

func NewTimberParams() *TimberParams {
	return &TimberParams{
		strings: make(map[string]string),
		text:    make(map[string]string),
		context: make(map[string]string),
		metrics: make(map[string]int),
	}
}

func (p *TimberParams) Strings(key string, val string) *TimberParams {
	p.strings[key] = val

	return p
}

func (p *TimberParams) StringsFromMap(stringsMap map[string]string) *TimberParams {
	for k, v := range stringsMap {
		p.strings[k] = v
	}

	return p
}

func (p *TimberParams) Text(key string, val string) *TimberParams {
	p.text[key] = val
	return p
}

func (p *TimberParams) TextFromMap(textMap map[string]string) *TimberParams {
	for k, v := range textMap {
		p.text[k] = v
	}

	return p
}

func (p *TimberParams) Context(key string, val string) *TimberParams {
	p.context[key] = val
	return p
}

func (p *TimberParams) ContextFromMap(contextMap map[string]string) *TimberParams {
	for k, v := range contextMap {
		p.context[k] = v
	}

	return p
}

func (p *TimberParams) Metrics(key string, val int) *TimberParams {
	p.metrics[key] = val
	return p
}

func (p *TimberParams) MetricsFromMap(metricsMap map[string]int) *TimberParams {
	for k, v := range metricsMap {
		p.metrics[k] = v
	}

	return p
}

func (p *TimberParams) addStaticParams(params map[string]string) {
	p.StringsFromMap(params)
}
